package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.DisabledAsset
import com.lykke.matching.engine.order.OrderStatus.InvalidFee
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.ReservedVolumeGreaterThanBalance
import com.lykke.matching.engine.order.OrderStatus.TooSmallVolume
import com.lykke.matching.engine.order.OrderStatus.UnknownAsset
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.matching.engine.utils.order.OrderStatusUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MarketOrderService(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                         private val genericLimitOrderService: GenericLimitOrderService,
                         assetsHolder: AssetsHolder,
                         private val assetsPairsHolder: AssetsPairsHolder,
                         private val balancesHolder: BalancesHolder,
                         private val assetSettingsCache: ApplicationSettingsCache,
                         private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                         private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                         private val orderBookQueue: BlockingQueue<OrderBook>,
                         private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                         private val rabbitSwapQueue: BlockingQueue<JsonSerializable>,
                         private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)
    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)


    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val feeInstruction: FeeInstruction?
        val feeInstructions: List<NewFeeInstruction>?
        val order = if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldMarketOrder
            LOGGER.debug("Got old market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, straight: ${message.straight}")
            feeInstruction = null
            feeInstructions = null
            MarketOrder(UUID.randomUUID().toString(), message.uid.toString(), message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight, message.reservedLimitVolume)
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
            feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
            feeInstructions = NewFeeInstruction.create(message.feesList)
            LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, straight: ${message.straight}, fee: $feeInstruction, fees: $feeInstructions")

            MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight, message.reservedLimitVolume, feeInstruction, listOfFee(feeInstruction, feeInstructions))
        }

        val assetPair = try {
            assetsPairsHolder.getAssetPair(order.assetPairId)
        } catch (e: Exception) {
            order.status = UnknownAsset.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Unknown asset: ${order.assetPairId}")
            writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status), order.assetPairId)
            return
        }

        if (assetSettingsCache.isAssetDisabled(assetPair.baseAssetId)
                || assetSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            order.status = DisabledAsset.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Disabled asset ${orderInfo(order)}")
            writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            return
        }

        if (!order.checkVolume(assetPair)) {
            order.status = TooSmallVolume.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Too small volume for ${orderInfo(order)}")
            writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            return
        }

        if (!checkFee(feeInstruction, feeInstructions)) {
            order.status = InvalidFee.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.error("Invalid fee (order id: ${order.id}, order externalId: ${order.externalId})")
            writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status), order.assetPairId)
            return
        }

        val orderBook = genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide())
        if (orderBook.size == 0) {
            order.status = NoLiquidity.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("No liquidity, no orders in order book, for ${orderInfo(order)}")
            writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            return
        }

        val matchingResult = matchingEngine.match(order, orderBook)
        when (OrderStatus.valueOf(matchingResult.order.status)) {
            NoLiquidity -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            }
            ReservedVolumeGreaterThanBalance -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status), "Reserved volume is higher than available balance")
            }
            NotEnoughFunds -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            }
            InvalidFee -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, OrderStatusUtils.toMessageStatus(order.status))
            }
            Matched -> {
                val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                    val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.toList())
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
                        LOGGER.error("${orderInfo(order)}: Unable to process cancelled orders wallet operations after matching: ${e.message}")
                    }
                    true
                } catch (e: BalanceException) {
                    order.status = OrderStatus.NotEnoughFunds.name
                    rabbitSwapQueue.put(MarketOrderWithTrades(order))
                    LOGGER.error("${orderInfo(order)}: Unable to process wallet operations after matching: ${e.message}")
                    writeResponse(messageWrapper, order, MessageStatus.LOW_BALANCE, e.message)
                    false
                }

                val clientLimitOrdersReport = LimitOrdersReport()
                val trustedClientLimitOrdersReport = LimitOrdersReport()
                if (preProcessResult) {
                    matchingResult.apply()
                    walletOperationsProcessor.apply(order.externalId, MessageType.MARKET_ORDER.name)
                    genericLimitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)
                    genericLimitOrderService.cancelLimitOrders(matchingResult.cancelledLimitOrders.toList())
                    orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult)

                    trustedClientLimitOrdersReport.orders.addAll(cancelledTrustedOrdersWithTrades)
                    clientLimitOrdersReport.orders.addAll(cancelledOrdersWithTrades)

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    genericLimitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)
                    genericLimitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                    lkkTradesQueue.put(matchingResult.lkkTrades)

                    rabbitSwapQueue.put(MarketOrderWithTrades(order, matchingResult.marketOrderTrades.toMutableList()))

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

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun orderInfo(order: MarketOrder) = "market order id: ${order.id}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, straight: ${order.straight}"

    private fun parseOld(array: ByteArray): ProtocolMessages.OldMarketOrder {
        return ProtocolMessages.OldMarketOrder.parseFrom(array)
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()).setRecordId(order.id).build())
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason).build())
            }
        } else {
            if (order.price != null) {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setPrice(order.price!!).setStatus(status.type).build())
            } else if (reason == null) {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setStatus(status.type).build())
            } else {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setStatus(status.type).setStatusReason(reason).build())
            }
        }
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message =  parseOld(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        } else {
            val message =  parse(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageWrapper.messageId!!.toLong()).build())
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        } else{
            messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        }
    }
}