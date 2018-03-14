package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.NotEnoughFundsLimitOrderCancelParams
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class LimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                          private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                          private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                          private val orderBookQueue: BlockingQueue<OrderBook>,
                          private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                          private val assetsHolder: AssetsHolder,
                          private val assetsPairsHolder: AssetsPairsHolder,
                          private val balancesHolder: BalancesHolder,
                          private val applicationSettingsCache: ApplicationSettingsCache,
                          private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                          private val matchingEngine: MatchingEngine,
                          private val LOGGER: Logger) {

    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)

    fun checkAndProcessStopOrder(assetPairId: String, now: Date) {
        val order = limitOrderService.getStopOrderForProcess(assetPairId) ?: return
        val orderBook = limitOrderService.getOrderBook(assetPairId)
        LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} (bestBidPrice=${orderBook.getBidPrice()}, bestAskPrice=${orderBook.getAskPrice()})")
        val payBackReserved = order.reservedLimitVolume!!
        order.reservedLimitVolume = null
        processLimitOrder(order, now, payBackReserved)
    }

    fun processLimitOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date): Boolean {
        val result = processLimitOrder(messageWrapper, order, isCancelOrders, now, null)
        checkAndProcessStopOrder(order.assetPairId, now)
        return result
    }

    fun processStopOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAsset = assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitVolume = if (order.isBuySide())
            RoundingUtils.round(order.volume * (order.upperPrice ?: order.lowerPrice)!!, limitAsset.accuracy, true)
        else
            order.getAbsVolume()

        val balance = balancesHolder.getBalance(order.clientId, limitAsset.assetId)
        val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset.assetId)
        val clientLimitOrdersReport = LimitOrdersReport()
        var cancelVolume = 0.0
        if (isCancelOrders) {
            limitOrderService.getAllPreviousStopOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                limitOrderService.cancelStopLimitOrder(orderToCancel.externalId)
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += orderToCancel.reservedLimitVolume!!
            }
        }

        val availableBalance = RoundingUtils.parseDouble(balancesHolder.getAvailableBalance(order.clientId, limitAsset.assetId, cancelVolume), limitAsset.accuracy).toDouble()
        try {
            validateStopLimitOrder(order, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info("${orderInfo(order)} ${e.message}")
            order.status = e.orderStatus.name
            val messageStatus = e.messageStatus
            if (cancelVolume > 0) {
                val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - cancelVolume, limitAsset.accuracy).toDouble()
                balancesHolder.updateReservedBalance(order.clientId, limitAsset.assetId, newReservedBalance)
                balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset.assetId, balance, balance, reservedBalance, newReservedBalance))))
            }

            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(messageStatus.type).build())

            clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
            clientLimitOrderReportQueue.put(clientLimitOrdersReport)
            return false
        }

        val orderBook = limitOrderService.getOrderBook(order.assetPairId)
        val bestBidPrice = orderBook.getBidPrice()
        val bestAskPrice = orderBook.getAskPrice()

        var price: Double? = null
        if (order.lowerLimitPrice != null && (order.isBuySide() && bestAskPrice > 0 && bestAskPrice <= order.lowerLimitPrice ||
                        !order.isBuySide() && bestBidPrice > 0 && bestBidPrice <= order.lowerLimitPrice)) {
            price = order.lowerPrice
        } else if(order.upperLimitPrice != null && (order.isBuySide() && bestAskPrice >= order.upperLimitPrice ||
                        !order.isBuySide() && bestBidPrice >= order.upperLimitPrice)) {
            price = order.upperPrice
        }

        if (price != null) {
            LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} immediately (bestBidPrice=$bestBidPrice, bestAskPrice=$bestAskPrice)")
            order.status = OrderStatus.InOrderBook.name
            order.price = price
            return processLimitOrder(order, now, 0.0)
        }

        order.reservedLimitVolume = limitVolume
        limitOrderService.addStopOrder(order)

        clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))

        val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - cancelVolume + limitVolume, limitAsset.accuracy).toDouble()
        balancesHolder.updateReservedBalance(order.clientId, limitAsset.assetId, newReservedBalance)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, now, listOf(ClientBalanceUpdate(order.clientId, limitAsset.assetId, balance, balance, reservedBalance, newReservedBalance))))

        writeResponse(messageWrapper, order, MessageStatus.OK)
        LOGGER.info("${orderInfo(order)} added to stop order book")

        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrderReportQueue.put(clientLimitOrdersReport)
        }

        return true
    }

    private fun processLimitOrder(order: NewLimitOrder, now: Date, payBackReserved: Double): Boolean {
        val result = processLimitOrder(null, order, false, now, payBackReserved)
        checkAndProcessStopOrder(order.assetPairId, now)
        return result
    }

    private fun processLimitOrder(messageWrapper: MessageWrapper?, order: NewLimitOrder, isCancelOrders: Boolean, now: Date, payBackReserved: Double?): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val limitAssetAccuracy = assetsHolder.getAsset(limitAsset).accuracy
        val limitVolume = if (order.isBuySide()) RoundingUtils.round(order.getAbsVolume() * order.price, assetsHolder.getAsset(limitAsset).accuracy, true) else order.getAbsVolume()

        val balance = balancesHolder.getBalance(order.clientId, limitAsset)
        val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

        val orderBook = limitOrderService.getOrderBook(order.assetPairId).copy()
        val clientLimitOrdersReport = LimitOrdersReport()
        val trustedClientLimitOrdersReport = LimitOrdersReport()

        var cancelVolume = 0.0
        if (isCancelOrders) {
            limitOrderService.getAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                limitOrderService.cancelLimitOrder(orderToCancel.externalId)
                orderBook.removeOrder(orderToCancel)
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += if (orderToCancel.isBuySide()) orderToCancel.remainingVolume * orderToCancel.price else orderToCancel.getAbsRemainingVolume()
            }
        }
        val totalPayBackReserved = cancelVolume + (payBackReserved ?: 0.0)

        val orderInfo = orderInfo(order)
        val availableBalance = RoundingUtils.parseDouble(balancesHolder.getAvailableBalance(order.clientId, limitAsset, totalPayBackReserved), limitAssetAccuracy).toDouble()

        try {
            validateLimitOrder(order, orderBook, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info("${orderInfo(order)} ${e.message}")
            order.status = e.orderStatus.name
            rejectOrder(reservedBalance, totalPayBackReserved, limitAsset, order, balance, clientLimitOrdersReport, orderBook, messageWrapper, e.messageStatus, now, isCancelOrders)
            return false
        }

        if (orderBook.leadToNegativeSpread(order)) {
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), availableBalance)
            val limitOrder = matchingResult.order as NewLimitOrder
            when (OrderStatus.valueOf(matchingResult.order.status)) {
                OrderStatus.NoLiquidity -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.NO_LIQUIDITY)
                }
                OrderStatus.ReservedVolumeGreaterThanBalance -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, "Reserved volume is higher than available balance")
                }
                OrderStatus.NotEnoughFunds -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.NOT_ENOUGH_FUNDS)
                }
                OrderStatus.InvalidFee -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.INVALID_FEE)
                }
                OrderStatus.Matched,
                OrderStatus.Processing -> {
                    limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)

                    val walletOperations = LinkedList<WalletOperation>()

                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        val result = limitOrderService.cancelNotEnoughFundsOrder(NotEnoughFundsLimitOrderCancelParams(matchingResult.cancelledLimitOrders.toList()))
                        walletOperations.addAll(result.walletOperation)
                        trustedClientLimitOrdersReport.orders.addAll(result.trustedClientLimitOrderWithTrades)
                        clientLimitOrdersReport.orders.addAll(result.clientLimitOrderWithTrades)
                    }

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    orderServiceHelper.processUncompletedOrder(matchingResult, assetPair, walletOperations)

                    limitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)
                    limitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                    lkkTradesQueue.put(matchingResult.lkkTrades)

                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder, matchingResult.marketOrderTrades.map { it ->
                        LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, it.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.feeInstruction, it.feeTransfer, it.fees)
                    }.toMutableList()))

                    if (matchingResult.limitOrdersReport != null) {
                        clientLimitOrdersReport.orders.addAll(matchingResult.limitOrdersReport.orders)
                    }

                    walletOperations.addAll(matchingResult.cashMovements)

                    if (order.status == OrderStatus.Processing.name) {
                        if (assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume) {
                            LOGGER.info("Order (id: ${order.externalId}) is cancelled due to min remaining volume (${RoundingUtils.roundForPrint(order.getAbsRemainingVolume())} < ${RoundingUtils.roundForPrint(assetPair.minVolume)})")
                            order.status = OrderStatus.Cancelled.name
                        } else {
                            order.reservedLimitVolume = if (order.isBuySide()) RoundingUtils.round(order.getAbsRemainingVolume() * order.price, limitAssetAccuracy, false) else order.getAbsRemainingVolume()
                            orderBook.addOrder(order)
                            limitOrderService.addOrder(order)
                            limitOrderService.setOrderBook(order.assetPairId, order.isBuySide(), orderBook.getOrderBook(order.isBuySide()))
                            limitOrderService.updateOrderBook(order.assetPairId, order.isBuySide())
                            limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), if (order.isBuySide()) orderBook.getBidPrice() else orderBook.getAskPrice(), matchingResult.timestamp))
                        }
                    }

                    if (order.reservedLimitVolume != null || totalPayBackReserved > 0.0) {
                        walletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, limitAsset, matchingResult.timestamp, 0.0, (order.reservedLimitVolume
                                ?: 0.0) - totalPayBackReserved))
                    }

                    balancesHolder.processWalletOperations(order.externalId, MessageType.LIMIT_ORDER.name, walletOperations)

                    val newOrderBook = OrderBook(limitOrder.assetPairId, limitOrder.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(limitOrder.assetPairId).getCopyOfOrderBook(limitOrder.isBuySide()))
                    orderBookQueue.put(newOrderBook)
                    rabbitOrderBookQueue.put(newOrderBook)
                    val oppositeOrderBook = OrderBook(limitOrder.assetPairId, !limitOrder.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(limitOrder.assetPairId).getCopyOfOrderBook(!limitOrder.isBuySide()))
                    orderBookQueue.put(oppositeOrderBook)
                    rabbitOrderBookQueue.put(oppositeOrderBook)
                    writeResponse(messageWrapper, order, MessageStatus.OK)
                }
                else -> {
                }
            }
            LOGGER.info("$orderInfo matched")
        } else {
            order.reservedLimitVolume = limitVolume

            orderBook.addOrder(order)
            limitOrderService.addOrder(order)
            limitOrderService.setOrderBook(order.assetPairId, order.isBuySide(), orderBook.getOrderBook(order.isBuySide()))
            limitOrderService.updateOrderBook(order.assetPairId, order.isBuySide())

            limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), if (order.isBuySide()) orderBook.getBidPrice() else orderBook.getAskPrice(), now))
            clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))

            val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - totalPayBackReserved + limitVolume, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
            balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance, reservedBalance, newReservedBalance))))

            val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
            orderBookQueue.put(rabbitOrderBook)
            rabbitOrderBookQueue.put(rabbitOrderBook)
            writeResponse(messageWrapper, order, MessageStatus.OK)
            LOGGER.info("$orderInfo added to order book")
        }

        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrderReportQueue.put(clientLimitOrdersReport)
        }
        if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
            trustedClientLimitOrderReportQueue.put(trustedClientLimitOrdersReport)
        }

        return true
    }

    private fun validateLimitOrder(order: NewLimitOrder, orderBook: AssetOrderBook, assetPair: AssetPair, availableBalance: Double, limitVolume: Double) {
        validateFee(order)
        validateAssets(assetPair)
        validatePrice(order)
        validateVolume(order)
        checkBalance(availableBalance, limitVolume)

        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException("${orderInfo(order)} lead to negative spread", OrderStatus.LeadToNegativeSpread, MessageStatus.LEAD_TO_NEGATIVE_SPREAD)
        }
    }

    private fun validateStopLimitOrder(order: NewLimitOrder, assetPair: AssetPair, availableBalance: Double, limitVolume: Double) {
        validateFee(order)
        validateAssets(assetPair)
        validateLimitPrices(order)
        validateVolume(order)
        checkBalance(availableBalance, limitVolume)
    }

    private fun validateFee(order: NewLimitOrder) {
        if (!checkFee(order.fee, order.fees)) {
            throw OrderValidationException("has invalid fee", OrderStatus.InvalidFee, MessageStatus.INVALID_FEE)
        }
    }

    private fun validateAssets(assetPair: AssetPair) {
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException("disabled asset", OrderStatus.DisabledAsset, MessageStatus.DISABLED_ASSET)
        }
    }

    private fun validatePrice(order: NewLimitOrder) {
        if (order.price <= 0.0) {
            throw OrderValidationException("price is invalid", OrderStatus.InvalidPrice, MessageStatus.INVALID_PRICE)
        }
    }

    private fun validateLimitPrices(order: NewLimitOrder) {
        var checked = false
        try {
            if ((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) return
            if ((order.upperLimitPrice == null).xor(order.upperPrice == null)) return
            if (order.lowerLimitPrice != null && (order.lowerLimitPrice <= 0.0 || order.lowerPrice!! <= 0.0)) return
            if (order.upperLimitPrice != null && (order.upperLimitPrice <= 0.0 || order.upperPrice!! <= 0.0)) return
            if (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice) return
            checked = true
        } finally {
            if (!checked) throw OrderValidationException("limit prices are invalid", OrderStatus.InvalidPrice, MessageStatus.INVALID_PRICE)
        }
    }

    private fun validateVolume(order: NewLimitOrder) {
        if (!order.checkVolume(assetsPairsHolder)) {
            throw OrderValidationException("volume is too small", OrderStatus.TooSmallVolume, MessageStatus.TOO_SMALL_VOLUME)
        }
    }

    private fun checkBalance(availableBalance: Double, limitVolume: Double) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException("not enough funds to reserve", OrderStatus.NotEnoughFunds, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE)
        }
    }

    private fun orderInfo(order: NewLimitOrder): String {
        val prefix = if (order.type == LimitOrderType.STOP_LIMIT) "Stop limit order" else "Limit order"
        return "$prefix (id: ${order.externalId})"
    }

    private fun rejectOrder(reservedBalance: Double, payBackReserved: Double, limitAsset: String, order: NewLimitOrder, balance: Double, trustedLimitOrdersReport: LimitOrdersReport, orderBook: AssetOrderBook, messageWrapper: MessageWrapper?, status: MessageStatus, now: Date, isCancelOrders: Boolean) {
        if (payBackReserved > 0) {
            val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - payBackReserved, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
            balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance, reservedBalance, newReservedBalance))))
        }

        trustedLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
        limitOrderService.setOrderBook(order.assetPairId, orderBook)

        if (messageWrapper != null) {
            if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
                messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(now.time).build())
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
            }
        }

        if (trustedLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrderReportQueue.put(trustedLimitOrdersReport)
        }

        if (isCancelOrders) {
            val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
            orderBookQueue.put(rabbitOrderBook)
            rabbitOrderBookQueue.put(rabbitOrderBook)
        }
    }

    private fun writeResponse(messageWrapper: MessageWrapper?, order: NewLimitOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper == null) {
            return
        }
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()).build())
        } else {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason).build())
            }
        }
    }
}