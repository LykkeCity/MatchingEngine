package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.InvalidFee
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.ReservedVolumeGreaterThanBalance
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.FeeInstruction
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MarketOrderService(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                         private val genericLimitOrderService: GenericLimitOrderService,
                         assetsHolder: AssetsHolder,
                         private val assetsPairsHolder: AssetsPairsHolder,
                         private val balancesHolder: BalancesHolder,
                         private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                         private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                         private val orderBookQueue: BlockingQueue<OrderBook>,
                         private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                         private val rabbitSwapQueue: BlockingQueue<JsonSerializable>,
                         private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                         genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory ?= null,
                         private val marketOrderValidator: MarketOrderValidator): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)
    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val now = Date()
        val feeInstruction: FeeInstruction?
        val feeInstructions: List<NewFeeInstruction>?
        val order = if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldMarketOrder
            LOGGER.debug("Got old market order messageId: ${messageWrapper.messageId}, " +
                    "id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.volume)}, straight: ${message.straight}")

            feeInstruction = null
            feeInstructions = null
            MarketOrder(UUID.randomUUID().toString(), message.uid.toString(), message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                    Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume))
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
            feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
            feeInstructions = NewFeeInstruction.create(message.feesList)
            LOGGER.debug("Got market order messageId: ${messageWrapper.messageId}, " +
                    "id: ${message.uid}, client: ${message.clientId}, " +
                    "asset: ${message.assetPairId}, volume: ${NumberUtils.roundForPrint(message.volume)}, " +
                    "straight: ${message.straight}, fee: $feeInstruction, fees: $feeInstructions")

            MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                    Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                    feeInstruction, listOfFee(feeInstruction, feeInstructions))
        }

        try {
            marketOrderValidator.performValidation(order, getOrderBook(order), feeInstruction, feeInstructions)
        } catch (e: OrderValidationException) {
            order.updateStatus(e.orderStatus, now)
            rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
            writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(e.orderStatus), e.message)
            return
        }

        val assetPair = getAssetPair(order)

        val matchingResult = matchingEngine.initTransaction().match(order, getOrderBook(order), messageWrapper.messageId!!)
        when (OrderStatus.valueOf(matchingResult.order.status)) {
            NoLiquidity -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
                writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status))
            }
            ReservedVolumeGreaterThanBalance -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
                writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), "Reserved volume is higher than available balance")
            }
            NotEnoughFunds -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
                writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status))
            }
            InvalidFee -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
                writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status))
            }
            Matched -> {
                val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                    val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.map {
                        val cancelledOrder = it.copy
                        cancelledOrder.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                        cancelledOrder
                    })
                    cancelledOrdersWalletOperations.addAll(result.walletOperations)
                    cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
                    cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
                }
                val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

                val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
                val preProcessResult = try {
                    walletOperationsProcessor.preProcess(matchingResult.ownCashMovements).preProcess(matchingResult.oppositeCashMovements, true)
                    try {
                        walletOperationsProcessor.preProcess(cancelledOrdersWalletOperations)
                    } catch (e: BalanceException) {
                        LOGGER.error("$order: Unable to process cancelled orders wallet operations after matching: ${e.message}")
                    }
                    true
                } catch (e: BalanceException) {
                    order.updateStatus(OrderStatus.NotEnoughFunds, matchingResult.timestamp)
                    rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order))
                    LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
                    writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), e.message)
                    false
                }

                val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
                val trustedClientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
                if (preProcessResult) {
                    matchingResult.apply()
                    val completedOrders = matchingResult.completedLimitOrders.map { it.origin!! }
                    val ordersToCancel = matchingResult.cancelledLimitOrders.map { it.origin!! }.toMutableList()
                    orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult, ordersToCancel)
                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
                    val ordersToSave = mutableListOf<LimitOrder>()
                    val ordersToRemove = mutableListOf<LimitOrder>()
                    ordersToRemove.addAll(completedOrders)
                    ordersToRemove.addAll(ordersToCancel)
                    val updatedOrders = matchingEngine.updatedOrders(matchingResult.orderBook, emptyList())
                    orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPair.assetPairId, !order.isBuySide(), updatedOrders.fullOrderBook))
                    ordersToSave.addAll(updatedOrders.updatedOrders)

                    messageWrapper.processedMessagePersisted = true
                    val updated = walletOperationsProcessor.persistBalances(messageWrapper.processedMessage(),
                            OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove),
                            null)
                    if (!updated) {
                        val message = "Unable to save result data"
                        LOGGER.error("$order: $message")
                        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), message)
                        return
                    }
                    walletOperationsProcessor.apply().sendNotification(order.externalId, MessageType.MARKET_ORDER.name, messageWrapper.messageId!!)


                    matchingEngine.apply()
                    genericLimitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders.map { it.origin!! })
                    genericLimitOrderService.cancelLimitOrders(ordersToCancel, matchingResult.timestamp)
                    genericLimitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)

                    trustedClientLimitOrdersReport.orders.addAll(cancelledTrustedOrdersWithTrades)
                    clientLimitOrdersReport.orders.addAll(cancelledOrdersWithTrades)



                    //genericLimitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                    lkkTradesQueue.put(matchingResult.lkkTrades)

                    rabbitSwapQueue.put(MarketOrderWithTrades(messageWrapper.messageId!!, order, matchingResult.marketOrderTrades.toMutableList()))

                    matchingResult.limitOrdersReport?.let {
                        clientLimitOrdersReport.orders.addAll(it.orders)
                    }

                    clientLimitOrderReportQueue.put(clientLimitOrdersReport)
                    if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
                        trustedClientLimitOrderReportQueue.put(trustedClientLimitOrdersReport)
                    }

                    val newOrderBook = OrderBook(order.assetPairId, !order.isBuySide(), order.matchedAt!!, genericLimitOrderService.getOrderBook(order.assetPairId).getCopyOfOrderBook(!order.isBuySide()))
                    orderBookQueue.put(newOrderBook)
                    rabbitOrderBookQueue.put(newOrderBook)
                    writeResponse(messageWrapper, order, MessageStatus.OK)
                }
            }
            else -> {
            }
        }

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!,
                assetPair.assetPairId,
                now)

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun getOrderBook(order: MarketOrder) =
            genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide())

    private fun getAssetPair(order: MarketOrder) = assetsPairsHolder.getAssetPair(order.assetPairId)

    private fun parseOld(array: ByteArray): ProtocolMessages.OldMarketOrder {
        return ProtocolMessages.OldMarketOrder.parseFrom(array)
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setRecordId(order.id))
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            val newResponseBuilder = ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(order.id)
                    .setStatus(status.type)

            if (reason != null) {
                newResponseBuilder.statusReason = reason
            }

            messageWrapper.writeNewResponse(newResponseBuilder)

        } else {
            val marketOrderResponse = ProtocolMessages.MarketOrderResponse.newBuilder()
                    .setStatus(status.type)

            if (order.price != null) {
                marketOrderResponse.price = order.price!!.toDouble()
            } else if (reason != null) {
                marketOrderResponse.statusReason = reason
            }

            messageWrapper.writeMarketOrderResponse(marketOrderResponse)
        }
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message =  parseOld(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message =  parse(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type))
        } else {
            messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder()
                    .setStatus(status.type))
        }
    }
}