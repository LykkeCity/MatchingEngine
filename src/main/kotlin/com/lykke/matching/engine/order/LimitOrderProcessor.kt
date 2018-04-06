package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
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
                          applicationSettingsCache: ApplicationSettingsCache,
                          private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                          private val matchingEngine: MatchingEngine,
                          private val LOGGER: Logger) {

    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)
    private val validator = LimitOrderValidator(assetsPairsHolder, applicationSettingsCache)

    fun processLimitOrder(messageWrapper: MessageWrapper?, order: NewLimitOrder, isCancelOrders: Boolean, now: Date, payBackReserved: Double?) {
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
            validateOrder(order, orderBook, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info("${orderInfo(order)} ${e.message}")
            order.status = e.orderStatus.name
            rejectOrder(reservedBalance, totalPayBackReserved, limitAsset, order, balance, clientLimitOrdersReport, orderBook, messageWrapper, e.messageStatus, now, isCancelOrders)
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            var isMatched = false
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
                    isMatched = true
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
                        LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, it.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.feeInstruction, it.feeTransfer, it.fees, it.absoluteSpread, it.relativeSpread)
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
            LOGGER.info(if (isMatched) "$orderInfo matched" else "$orderInfo got status: ${order.status}")
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
    }

    private fun validateOrder(order: NewLimitOrder, orderBook: AssetOrderBook, assetPair: AssetPair, availableBalance: Double, limitVolume: Double) {
        validator.validateFee(order)
        validator.validateAssets(assetPair)
        validator.validatePrice(order)
        validator.validateVolume(order)
        validator.checkBalance(availableBalance, limitVolume)

        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException("${orderInfo(order)} lead to negative spread", OrderStatus.LeadToNegativeSpread, MessageStatus.LEAD_TO_NEGATIVE_SPREAD)
        }
    }

    private fun orderInfo(order: NewLimitOrder): String {
        return "Limit order (id: ${order.externalId})"
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