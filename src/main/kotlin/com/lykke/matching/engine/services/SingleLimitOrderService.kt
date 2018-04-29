
package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.OrderStatusUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue
class SingleLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                              private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                              private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                              private val orderBookQueue: BlockingQueue<OrderBook>,
                              private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                              private val assetsHolder: AssetsHolder,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val balancesHolder: BalancesHolder,
                              private val applicationSettingsCache: ApplicationSettingsCache,
                              private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)
    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()

        val order: NewLimitOrder

        val now = Date()
        val isCancelOrders: Boolean
        val clientLimitOrdersReport = LimitOrdersReport()
        val trustedClientLimitOrdersReport = LimitOrdersReport()

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val feeInstruction: LimitOrderFeeInstruction?
        val feeInstructions: List<NewLimitOrderFeeInstruction>?
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrder
            val uid = UUID.randomUUID().toString()
            order = NewLimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, oldMessage.volume,
                    oldMessage.price, OrderStatus.InOrderBook.name, Date(oldMessage.timestamp), now, oldMessage.volume, null)

            LOGGER.info("""Got old limit order id: ${oldMessage.uid}, client ${oldMessage.clientId},
                |assetPair: ${oldMessage.assetPairId}, volume: ${NumberUtils.roundForPrint(oldMessage.volume)},
                |price: ${NumberUtils.roundForPrint(oldMessage.price)},
                |cancel: ${oldMessage.cancelAllPreviousLimitOrders}""".trimMargin())

            isCancelOrders = oldMessage.cancelAllPreviousLimitOrders
            feeInstruction = null
            feeInstructions = null
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrder
            val uid = UUID.randomUUID().toString()
            feeInstruction = if (message.hasFee()) LimitOrderFeeInstruction.create(message.fee) else null
            feeInstructions = NewLimitOrderFeeInstruction.create(message.feesList)
            order = NewLimitOrder(uid, message.uid, message.assetPairId, message.clientId, message.volume,
                    message.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, message.volume, null,
                    fee = feeInstruction, fees = listOfLimitOrderFee(feeInstruction, feeInstructions))

            LOGGER.info("Got limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, volume: ${NumberUtils.roundForPrint(message.volume)}, price: ${NumberUtils.roundForPrint(message.price)}, cancel: ${message.cancelAllPreviousLimitOrders}, fee: $feeInstruction, fees: $feeInstructions")

            isCancelOrders = message.cancelAllPreviousLimitOrders
        }

        val assetPair = getAssetPair(order)
        val limitAsset = getLimitAsset(order)
        val limitAssetAccuracy = getLimitAssetAccuracy(limitAsset)
        val limitVolume = getLimitVolume(order, limitAssetAccuracy)

        val balance = getBalance(order)
        val reservedBalance = getReservedBalance(order)

        val orderBook = limitOrderService.getOrderBook(order.assetPairId).copy()

        var cancelVolume = 0.0
        if (isCancelOrders) {
            limitOrderService.getAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                limitOrderService.cancelLimitOrder(orderToCancel.externalId)
                orderBook.removeOrder(orderToCancel)
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += if (orderToCancel.isBuySide()) orderToCancel.remainingVolume * orderToCancel.price else orderToCancel.getAbsRemainingVolume()
            }
        }


        val orderInfo = getOrderInfo(order)

        if (!performValidation(order, cancelVolume, clientLimitOrdersReport,
                orderBook, messageWrapper, now, isCancelOrders, feeInstruction, feeInstructions)) {
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            var isMatched = false
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide())/*, autoSuccessApply = false*/)
            val orderCopy = matchingResult.order as NewLimitOrder
            when (OrderStatus.valueOf(orderCopy.status)) {
                OrderStatus.NoLiquidity -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
                }
                OrderStatus.ReservedVolumeGreaterThanBalance -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status), "Reserved volume is higher than available balance")
                }
                OrderStatus.NotEnoughFunds -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
                }
                OrderStatus.InvalidFee -> {
                    clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
                }
                OrderStatus.Matched,
                OrderStatus.Processing-> {
                    isMatched = true
                    val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                    val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                    val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        val result = limitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.toList())
                        cancelledOrdersWalletOperations.addAll(result.walletOperations)
                        cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
                        cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
                    }
                    val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

                    val ownWalletOperations = LinkedList<WalletOperation>(matchingResult.ownCashMovements)
                    if (orderCopy.status == OrderStatus.Processing.name) {
                        if (assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume) {
                            LOGGER.info("$orderInfo:  Cancelled due to min remaining volume (${NumberUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
                            orderCopy.status = OrderStatus.Cancelled.name
                        } else {
                            orderCopy.reservedLimitVolume = if (orderCopy.isBuySide()) NumberUtils.round(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAssetAccuracy, false) else orderCopy.getAbsRemainingVolume()
                            val newReservedBalance = NumberUtils.parseDouble(orderCopy.reservedLimitVolume!! - cancelVolume, limitAssetAccuracy).toDouble()
                            ownWalletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, orderCopy.clientId, limitAsset, matchingResult.timestamp, 0.0, newReservedBalance))
                        }
                    }

                    val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
                    val preProcessResult = try {
                        walletOperationsProcessor.preProcess(ownWalletOperations).preProcess(matchingResult.oppositeCashMovements, true)
                        try {
                            walletOperationsProcessor.preProcess(cancelledOrdersWalletOperations)
                        } catch (e: BalanceException) {
                            LOGGER.error("$orderInfo: Unable to process cancelled orders wallet operations after matching: ${e.message}")
                        }
                        true
                    } catch (e: BalanceException) {
                        order.status = OrderStatus.NotEnoughFunds.name
                        clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                        LOGGER.error("$orderInfo: Unable to process wallet operations after matching: ${e.message}")
                        writeResponse(messageWrapper, order, MessageStatus.LOW_BALANCE, e.message)
                        false
                    }

                    if (preProcessResult) {
                        matchingResult.apply()
                        walletOperationsProcessor.apply(order.externalId, MessageType.LIMIT_ORDER.name)
                        limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)
                        limitOrderService.cancelLimitOrders(matchingResult.cancelledLimitOrders.toList())
                        orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult)

                        trustedClientLimitOrdersReport.orders.addAll(cancelledTrustedOrdersWithTrades)
                        clientLimitOrdersReport.orders.addAll(cancelledOrdersWithTrades)

                        matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                        limitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)
                        limitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                        lkkTradesQueue.put(matchingResult.lkkTrades)

                        clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order, matchingResult.marketOrderTrades.map { it ->
                            LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, it.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.feeInstruction, it.feeTransfer, it.fees, it.absoluteSpread, it.relativeSpread)
                        }.toMutableList()))

                        if (matchingResult.limitOrdersReport != null) {
                            clientLimitOrdersReport.orders.addAll(matchingResult.limitOrdersReport.orders)
                        }

                        if (order.status == OrderStatus.Processing.name) {
                            orderBook.addOrder(order)
                            limitOrderService.addOrder(order)
                            limitOrderService.setOrderBook(order.assetPairId, order.isBuySide(), orderBook.getOrderBook(order.isBuySide()))
                            limitOrderService.updateOrderBook(order.assetPairId, order.isBuySide())
                            limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), if (order.isBuySide()) orderBook.getBidPrice() else orderBook.getAskPrice(), matchingResult.timestamp))
                        }

                        val newOrderBook = OrderBook(order.assetPairId, order.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(order.assetPairId).getCopyOfOrderBook(order.isBuySide()))
                        orderBookQueue.put(newOrderBook)
                        rabbitOrderBookQueue.put(newOrderBook)
                        val oppositeOrderBook = OrderBook(order.assetPairId, !order.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(order.assetPairId).getCopyOfOrderBook(!order.isBuySide()))
                        orderBookQueue.put(oppositeOrderBook)
                        rabbitOrderBookQueue.put(oppositeOrderBook)
                        writeResponse(messageWrapper, order, MessageStatus.OK)
                    }
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

            val newReservedBalance = NumberUtils.parseDouble(reservedBalance - cancelVolume + limitVolume, limitAssetAccuracy).toDouble()
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

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun isPriceAccuracyValid(order: NewLimitOrder, cancelVolume: Double,
                                     clientLimitOrdersReport: LimitOrdersReport, orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                                     now: Date, isCancelOrders: Boolean): Boolean{
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.price, getAssetPair(order).accuracy)

        if (!priceAccuracyValid) {
            LOGGER.info("${getOrderInfo(order)} invalid price accuracy")
            order.status = OrderStatus.InvalidPriceAccuracy.name
            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.INVALID_PRICE_ACCURACY, now, isCancelOrders)
        }

        return priceAccuracyValid
    }

    private fun isVolumeAccuracyValid(order: NewLimitOrder,
                                      cancelVolume: Double, clientLimitOrdersReport: LimitOrdersReport,
                                      orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                                      now: Date, isCancelOrders: Boolean): Boolean {
        val baseAssetAccuracy = getBaseAssetAccuracy(order)
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetAccuracy)

        if (!volumeAccuracyValid) {
            LOGGER.info("${getOrderInfo(order)} invalid volume accuracy")
            order.status = OrderStatus.InvalidVolumeAccuracy.name
            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.INVALID_VOLUME_ACCURACY, now, isCancelOrders)
        }

        return volumeAccuracyValid
    }

    private fun getLimitVolume(order: NewLimitOrder, limitAssetAccuracy: Int) =
            if (order.isBuySide()) NumberUtils.round(order.getAbsVolume() * order.price, limitAssetAccuracy, true) else order.getAbsVolume()

    private fun getOrderInfo(order: NewLimitOrder): String {
        return """Limit order id: ${order.externalId}, client ${order.clientId},
                |assetPair: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)},
                |price: ${NumberUtils.roundForPrint(order.price)}""".trimMargin()
    }

    private fun performValidation(order: NewLimitOrder, cancelVolume: Double,
                                  clientLimitOrdersReport: LimitOrdersReport,
                                  orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                                  now: Date, isCancelOrders: Boolean, feeInstruction: LimitOrderFeeInstruction?,
                                  feeInstructions: List<NewLimitOrderFeeInstruction>?): Boolean {

        val validations = arrayOf(
                {isAssetEnabled(order, cancelVolume,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isPriceAccuracyValid(order, cancelVolume,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isPriceValid(order, cancelVolume,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isVolumeValid(order, cancelVolume,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isVolumeAccuracyValid(order, cancelVolume,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isEnoughFunds( cancelVolume, order,
                        clientLimitOrdersReport, orderBook, messageWrapper, now, isCancelOrders)},

                {isSpreadValid(orderBook, order, cancelVolume,
                        clientLimitOrdersReport, messageWrapper, now, isCancelOrders)},

                {isFeeValid(feeInstruction, feeInstructions,
                        order, cancelVolume, clientLimitOrdersReport,
                        orderBook, messageWrapper, now, isCancelOrders)
                })

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun isSpreadValid(orderBook: AssetOrderBook, order: NewLimitOrder, cancelVolume: Double,
                              clientLimitOrdersReport: LimitOrdersReport,
                              messageWrapper: MessageWrapper, now: Date, isCancelOrders: Boolean): Boolean {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            LOGGER.info("${getOrderInfo(order)} lead to negative spread")
            order.status = OrderStatus.LeadToNegativeSpread.name

            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.LEAD_TO_NEGATIVE_SPREAD, now, isCancelOrders)
            return false
        }

        return true
    }

    private fun isEnoughFunds(cancelVolume: Double, order: NewLimitOrder,
                              clientLimitOrdersReport: LimitOrdersReport, orderBook: AssetOrderBook,
                              messageWrapper: MessageWrapper, now: Date, isCancelOrders: Boolean): Boolean {
        val reservedBalance = getReservedBalance(order)
        val limitAssetAccuracy = getLimitAssetAccuracy(getLimitAsset(order))
        val balance = getBalance(order)
        val limitVolume = getLimitVolume(order, limitAssetAccuracy)

        if (NumberUtils.parseDouble(balance - (reservedBalance - cancelVolume), limitAssetAccuracy).toDouble() < limitVolume) {
            LOGGER.info("${getOrderInfo(order)} not enough funds to reserve")
            order.status = OrderStatus.NotEnoughFunds.name

            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, now, isCancelOrders)

            return false
        }

        return true
    }

    private fun isVolumeValid(order: NewLimitOrder,
                              cancelVolume: Double, clientLimitOrdersReport: LimitOrdersReport,
                              orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                              now: Date, isCancelOrders: Boolean): Boolean {
        if (!order.checkVolume(assetsPairsHolder)) {
            LOGGER.info("${getOrderInfo(order)} volume is too small")
            order.status = OrderStatus.TooSmallVolume.name

            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.TOO_SMALL_VOLUME, now, isCancelOrders)
            return false
        }

        return true
    }

    private fun isAssetEnabled(order: NewLimitOrder, cancelVolume: Double,
                               clientLimitOrdersReport: LimitOrdersReport,
                               orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                               now: Date, isCancelOrders: Boolean): Boolean {
        val assetPair = getAssetPair(order)
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId)
                || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            LOGGER.info("${getOrderInfo(order)}: disabled asset")
            order.status = OrderStatus.DisabledAsset.name
            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.DISABLED_ASSET, now, isCancelOrders)

            return false
        }

        return true
    }

    private fun isPriceValid(order: NewLimitOrder,
                             cancelVolume: Double, clientLimitOrdersReport: LimitOrdersReport,
                             orderBook: AssetOrderBook, messageWrapper: MessageWrapper,
                             now: Date, isCancelOrders: Boolean): Boolean {
        if (order.price <= 0.0) {
            LOGGER.info("${getOrderInfo(order)} price is invalid")
            order.status = OrderStatus.InvalidPrice.name

            rejectOrder(cancelVolume, order, clientLimitOrdersReport,
                    orderBook, messageWrapper, MessageStatus.INVALID_PRICE, now, isCancelOrders)
            return false
        }

        return true
    }

    private fun isFeeValid(feeInstruction: LimitOrderFeeInstruction?, feeInstructions: List<NewLimitOrderFeeInstruction>?,
                           order: NewLimitOrder, cancelVolume: Double,
                           clientLimitOrdersReport: LimitOrdersReport, orderBook: AssetOrderBook,
                           messageWrapper: MessageWrapper, now: Date, isCancelOrders: Boolean): Boolean {
        if (!checkFee(feeInstruction, feeInstructions)) {
            LOGGER.info("""Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId},
                    |volume: ${NumberUtils.roundForPrint(order.volume)},
                    |price: ${NumberUtils.roundForPrint(order.price)} has invalid fee""".trimMargin())
            order.status = OrderStatus.InvalidFee.name
            rejectOrder(cancelVolume, order, clientLimitOrdersReport, orderBook,
                    messageWrapper, MessageStatus.INVALID_FEE,
                    now, isCancelOrders)

            if (isCancelOrders) {
                val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(),
                        now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
                orderBookQueue.put(rabbitOrderBook)
                rabbitOrderBookQueue.put(rabbitOrderBook)
            }
            return false
        }

        return true
    }

    private fun getBalance(order: NewLimitOrder): Double  {
        val limitAsset = getLimitAsset(order)
        return balancesHolder.getBalance(order.clientId, limitAsset)
    }

    private fun getReservedBalance(order: NewLimitOrder): Double {
        val limitAsset = getLimitAsset(order)
        return balancesHolder.getReservedBalance(order.clientId, limitAsset)
    }

    private fun getLimitAsset(order: NewLimitOrder): String {
        val assetPair = getAssetPair(order)
        return if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
    }

    private fun getAssetPair(order: NewLimitOrder): AssetPair {
        return assetsPairsHolder.getAssetPair(order.assetPairId)
    }

    private fun rejectOrder(cancelVolume: Double,
                            order: NewLimitOrder,
                            trustedLimitOrdersReport: LimitOrdersReport,
                            orderBook: AssetOrderBook,
                            messageWrapper: MessageWrapper,
                            status: MessageStatus,
                            now: Date,
                            isCancelOrders: Boolean) {
        val reservedBalance = getReservedBalance(order)
        val limitAsset = getLimitAsset(order)
        val balance = getBalance(order)

        if (cancelVolume > 0) {
            val newReservedBalance = NumberUtils.parseDouble(reservedBalance - cancelVolume, getLimitAssetAccuracy(limitAsset)).toDouble()
            balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
            balancesHolder.sendBalanceUpdate(
                    BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(),
                            listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance,
                                    reservedBalance, newReservedBalance))))
        }

        trustedLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
        limitOrderService.setOrderBook(order.assetPairId, orderBook)

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(now.time).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
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

    private fun getLimitAssetAccuracy(limitAsset: String) = assetsHolder.getAsset(limitAsset).accuracy

    private fun getBaseAssetAccuracy(order: NewLimitOrder) =
            assetsHolder.getAsset(assetsPairsHolder.getAssetPair(order.assetPairId).baseAssetId).accuracy


    private fun writeResponse(messageWrapper: MessageWrapper, order: NewLimitOrder, status: MessageStatus, reason: String? = null) {
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

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val message =  parseOldLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageWrapper.messageId!!.toLong()).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        }
    }
}
