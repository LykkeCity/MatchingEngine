package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
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
import com.lykke.matching.engine.order.OrderStatus.InvalidValue
import com.lykke.matching.engine.order.OrderStatus.InvalidVolume
import com.lykke.matching.engine.order.OrderStatus.InvalidVolumeAccuracy
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.ReservedVolumeGreaterThanBalance
import com.lykke.matching.engine.order.OrderStatus.TooHighPriceDeviation
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
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
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.order.cancel.CancelOrdersUtils
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

@Service
class MarketOrderService @Autowired constructor(
        private val genericLimitOrderService: GenericLimitOrderService,
        private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
        assetsHolder: AssetsHolder,
        private val assetsPairsHolder: AssetsPairsHolder,
        private val balancesHolder: BalancesHolder,
        private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
        private val orderBookQueue: BlockingQueue<OrderBook>,
        private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
        private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
        genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory? = null,
        private val marketOrderValidator: MarketOrderValidator,
        feeProcessor: FeeProcessor,
        private val settings: ApplicationSettingsCache,
        private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
        private val messageSender: MessageSender): AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, genericLimitOrderService, assetsHolder, assetsPairsHolder, feeProcessor)
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
        val parsedMessage = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
        feeInstruction = if (parsedMessage.hasFee()) FeeInstruction.create(parsedMessage.fee) else null
        feeInstructions = NewFeeInstruction.create(parsedMessage.feesList)
        LOGGER.debug("Got market order messageId: ${messageWrapper.messageId}, " +
                "id: ${parsedMessage.uid}, client: ${parsedMessage.clientId}, " +
                "asset: ${parsedMessage.assetPairId}, volume: ${NumberUtils.roundForPrint(parsedMessage.volume)}, " +
                "straight: ${parsedMessage.straight}, fee: $feeInstruction, fees: $feeInstructions")

        val order = MarketOrder(UUID.randomUUID().toString(), parsedMessage.uid, parsedMessage.assetPairId, parsedMessage.clientId, BigDecimal.valueOf(parsedMessage.volume), null,
                Processing.name, now, Date(parsedMessage.timestamp), now, null, parsedMessage.straight, BigDecimal.valueOf(parsedMessage.reservedLimitVolume),
                feeInstruction, listOfFee(feeInstruction, feeInstructions))

        try {
            marketOrderValidator.performValidation(order, getOrderBook(order), feeInstruction, feeInstructions)
        } catch (e: OrderValidationException) {
            order.updateStatus(e.orderStatus, now)
            sendErrorNotification(messageWrapper, order, now)
            writeErrorResponse(messageWrapper, order, e.message)
            return
        }

        val assetPair = getAssetPair(order)

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
        val matchingResult = matchingEngine.initTransaction(walletOperationsProcessor).match(order,
                getOrderBook(order),
                messageWrapper.messageId!!,
                priceDeviationThreshold = assetPair.marketOrderPriceDeviationThreshold ?: settings.marketOrderPriceDeviationThreshold(assetPair.assetPairId))

        when (OrderStatus.valueOf(matchingResult.order.status)) {
            ReservedVolumeGreaterThanBalance -> {
                processRejectedMatchingResult(messageWrapper, matchingResult, now, "Reserved volume is higher than available balance")
            }
            NoLiquidity,
            NotEnoughFunds,
            InvalidFee,
            InvalidVolumeAccuracy,
            InvalidVolume,
            InvalidValue,
            TooHighPriceDeviation -> {
                processRejectedMatchingResult(messageWrapper, matchingResult, now)
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
                    LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
                    sendErrorNotification(messageWrapper, order, now)
                    writeErrorResponse(messageWrapper, order, e.message)
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

                    trustedClientLimitOrdersReport.orders.addAll(cancelledTrustedOrdersWithTrades)

                    val sequenceNumber = messageSequenceNumberHolder.getNewValue()
                    val trustedClientsSequenceNumber = if (trustedClientLimitOrdersReport.orders.isNotEmpty())
                        messageSequenceNumberHolder.getNewValue() else null

                    val updated = walletOperationsProcessor.persistBalances(messageWrapper.processedMessage,
                            OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove),
                            null,
                            trustedClientsSequenceNumber ?: sequenceNumber)
                    messageWrapper.triedToPersist = true
                    messageWrapper.persisted = updated
                    if (!updated) {
                        writePersistenceErrorResponse(messageWrapper, order)
                        return
                    }
                    walletOperationsProcessor.apply().sendNotification(order.externalId, MessageType.MARKET_ORDER.name, messageWrapper.messageId!!)


                    matchingEngine.apply()
                    genericLimitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders.map { it.origin!! })
                    genericLimitOrderService.cancelLimitOrders(ordersToCancel, matchingResult.timestamp)
                    genericLimitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)

                    clientLimitOrdersReport.orders.addAll(cancelledOrdersWithTrades)

                    lkkTradesQueue.put(matchingResult.lkkTrades)

                    val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order, matchingResult.marketOrderTrades.toMutableList())
                    rabbitSwapQueue.put(marketOrderWithTrades)

                    matchingResult.limitOrdersReport?.let {
                        clientLimitOrdersReport.orders.addAll(it.orders)
                    }

                    clientLimitOrdersQueue.put(clientLimitOrdersReport)
                    if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
                        trustedClientsLimitOrdersQueue.put(trustedClientLimitOrdersReport)
                    }

                    val outgoingMessage = EventFactory.createExecutionEvent(sequenceNumber,
                            messageWrapper.messageId!!,
                            messageWrapper.id!!,
                            now,
                            MessageType.MARKET_ORDER,
                            walletOperationsProcessor.getClientBalanceUpdates(),
                            clientLimitOrdersReport.orders,
                            marketOrderWithTrades)
                    messageSender.sendMessage(outgoingMessage)

                    if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
                        val trustedClientsOutgoingMessage = EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                                messageWrapper.messageId!!,
                                messageWrapper.id!!,
                                now,
                                MessageType.MARKET_ORDER,
                                trustedClientLimitOrdersReport.orders)
                        messageSender.sendTrustedClientsMessage(trustedClientsOutgoingMessage)
                    }

                    val orderBook = genericLimitOrderService.getOrderBook(order.assetPairId)
                    val newOrderBook = OrderBook(order.assetPairId, !order.isBuySide(), order.matchedAt!!, orderBook.getCopyOfOrderBook(!order.isBuySide()))
                    genericLimitOrderService.putTradeInfo(TradeInfo(order.assetPairId, !order.isBuySide(), if (order.isBuySide()) orderBook.getAskPrice() else orderBook.getBidPrice(), now))
                    orderBookQueue.put(newOrderBook)
                    rabbitOrderBookQueue.put(newOrderBook)
                    writeResponse(messageWrapper, order, MessageStatus.OK)
                }
            }
            else -> {
            }
        }

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair, now)

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

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun processRejectedMatchingResult(messageWrapper: MessageWrapper,
                                              matchingResult: MatchingResult,
                                              now: Date,
                                              statusReason: String? = null) {

        val order = matchingResult.order as MarketOrder
        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            val genericLimitOrdersCanceller = genericLimitOrdersCancellerFactory.create(LOGGER, now)
                    .preProcessLimitOrders(matchingResult.cancelledLimitOrders.map { it.origin!! })

            val updated = CancelOrdersUtils.persistAndSendNotifications(genericLimitOrdersCanceller,
                    order.externalId,
                    messageWrapper.messageId!!,
                    messageWrapper.processedMessage,
                    MessageType.MARKET_ORDER,
                    false,
                    balancesHolder,
                    messageSequenceNumberHolder,
                    clientLimitOrdersQueue,
                    trustedClientsLimitOrdersQueue,
                    messageSender,
                    now,
                    order,
                    rabbitSwapQueue)
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = updated
            if (!updated) {
                writePersistenceErrorResponse(messageWrapper, order)
                return
            }
        } else {
            sendErrorNotification(messageWrapper, order, now)
        }

        writeErrorResponse(messageWrapper,
                order,
                statusReason)
    }

    private fun writePersistenceErrorResponse(messageWrapper: MessageWrapper, order: MarketOrder) {
        val message = "Unable to save result data"
        LOGGER.error("$order: $message")
        writeResponse(messageWrapper, order, MessageStatus.RUNTIME, message)
        return
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   order: MarketOrder,
                                   statusReason: String? = null) {
        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), statusReason)
    }

    private fun sendErrorNotification(messageWrapper: MessageWrapper,
                                      order: MarketOrder,
                                      now: Date) {
        val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
        rabbitSwapQueue.put(marketOrderWithTrades)
        val outgoingMessage = EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                messageWrapper.messageId!!,
                messageWrapper.id!!,
                now,
                MessageType.MARKET_ORDER,
                marketOrderWithTrades)
        messageSender.sendMessage(outgoingMessage)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        val marketOrderResponse = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type)
        if (order.price != null) {
            marketOrderResponse.price = order.price!!.toDouble()
        } else if (reason != null) {
            marketOrderResponse.statusReason = reason
        }
        messageWrapper.writeMarketOrderResponse(marketOrderResponse)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type))
    }
}