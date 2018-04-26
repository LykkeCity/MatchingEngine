package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
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
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.util.*
import java.util.concurrent.BlockingQueue

class MarketOrderService(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                         private val genericLimitOrderService: GenericLimitOrderService,
                         private val assetsHolder: AssetsHolder,
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
            LOGGER.debug("""Got old market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId},
                |volume: ${NumberUtils.roundForPrint(message.volume)}, straight: ${message.straight}""".trimMargin())
            feeInstruction = null
            feeInstructions = null
            MarketOrder(UUID.randomUUID().toString(), message.uid.toString(), message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight, message.reservedLimitVolume)
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
            feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
            feeInstructions = NewFeeInstruction.create(message.feesList)
            LOGGER.debug("""Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId},
                | volume: ${NumberUtils.roundForPrint(message.volume)}, straight: ${message.straight},
                | fee: $feeInstruction, fees: $feeInstructions""".trimMargin())

            MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight,
                    message.reservedLimitVolume, feeInstruction, listOfFee(feeInstruction, feeInstructions))
        }

        if (!performValidation(messageWrapper, order, feeInstruction, feeInstructions)) {
            return
        }

        val assetPair = getAssetPair(order)
        val orderBook = getOrderBook(order)

        val matchingResult = matchingEngine.match(order, orderBook)
        when (OrderStatus.valueOf(matchingResult.order.status)) {
            NoLiquidity -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, MessageStatus.NO_LIQUIDITY)
            }
            ReservedVolumeGreaterThanBalance -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, "Reserved volume is higher than available balance")
            }
            NotEnoughFunds -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, MessageStatus.NOT_ENOUGH_FUNDS)
            }
            OrderStatus.InvalidFee -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(order))
                writeResponse(messageWrapper, order, MessageStatus.INVALID_FEE)
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

    private fun getOrderBook(order: MarketOrder) =
            genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide())

    private fun isOrderBookValid(messageWrapper: MessageWrapper, order: MarketOrder): Boolean {
        val orderBook = getOrderBook(order)
        if (orderBook.size == 0) {
            order.status = NoLiquidity.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("No liquidity, no orders in order book, for ${orderInfo(order)}")
            writeResponse(messageWrapper, order, MessageStatus.NO_LIQUIDITY)
            return false
        }

        return true
    }

    private fun isFeeValid(messageWrapper: MessageWrapper, feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?,
                           order: MarketOrder): Boolean {
        if (!checkFee(feeInstruction, feeInstructions)) {
            order.status = InvalidFee.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.error("Invalid fee (order id: ${order.id}, order externalId: ${order.externalId})")
            writeResponse(messageWrapper, order, MessageStatus.INVALID_FEE, order.assetPairId)
            return false
        }

        return true
    }

    private fun isVolumeValid(messageWrapper: MessageWrapper, order: MarketOrder): Boolean {
        val assetPair = getAssetPair(order)
        if (!order.checkVolume(assetPair)) {
            order.status = TooSmallVolume.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Too small volume for ${orderInfo(order)}")
            writeResponse(messageWrapper, order, MessageStatus.TOO_SMALL_VOLUME)
            return false
        }

        return true
    }

    private fun isAssetEnabled(messageWrapper: MessageWrapper, order: MarketOrder): Boolean {
        val assetPair = getAssetPair(order)
        if (assetSettingsCache.isAssetDisabled(assetPair.baseAssetId)
                || assetSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            order.status = DisabledAsset.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Disabled asset ${orderInfo(order)}")
            writeResponse(messageWrapper, order, MessageStatus.DISABLED_ASSET)
            return false
        }
        return true
    }

    private fun isAssetKnown(order: MarketOrder, messageWrapper: MessageWrapper): Boolean {
        try {
            getAssetPair(order)
        } catch (e: Exception) {
            order.status = UnknownAsset.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.warn("Exception fetching asset", e)
            LOGGER.info("Unknown asset: ${order.assetPairId}")
            writeResponse(messageWrapper, order, MessageStatus.UNKNOWN_ASSET, order.assetPairId)
            return false
        }

        return true
    }

    private fun getBaseAsset(order: MarketOrder): String {
        val assetPair = getAssetPair(order)
        return if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId
    }

    private fun isVolumeAccuracyValid(messageWrapper: MessageWrapper, order: MarketOrder): Boolean {
        val baseAssetVolumeAccuracy = assetsHolder.getAsset(getBaseAsset(order)).accuracy
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetVolumeAccuracy)

        if (!volumeAccuracyValid) {
            order.status = OrderStatus.InvalidVolumeAccuracy.name
            LOGGER.info("Volume accuracy invalid form base assetId: $baseAssetVolumeAccuracy, volume: ${order.volume}")
            writeResponse(messageWrapper, order, MessageStatus.INVALID_VOLUME_ACCURACY, order.assetPairId)
        }

        return volumeAccuracyValid
    }

    private fun isPriceAccuracyValid(messageWrapper: MessageWrapper, order: MarketOrder): Boolean {
        val price = order.price ?: return true

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(price, getAssetPair(order).accuracy)

        if (!priceAccuracyValid) {
            order.status = OrderStatus.InvalidPriceAccuracy.name
            LOGGER.info("Invalid order accuracy, ${order.assetPairId}, price: ${order.price}")
            writeResponse(messageWrapper, order, MessageStatus.INVALID_PRICE_ACCURACY, order.assetPairId)
        }

        return priceAccuracyValid
    }

    fun performValidation(messageWrapper: MessageWrapper, order: MarketOrder, feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?): Boolean {
        val validations = arrayOf({isAssetKnown(order, messageWrapper)},
                {isAssetEnabled(messageWrapper, order)},
                {isVolumeValid(messageWrapper, order)},
                {isFeeValid(messageWrapper, feeInstruction, feeInstructions, order)},
                {isOrderBookValid(messageWrapper, order)},
                {isVolumeAccuracyValid(messageWrapper, order)},
                {isPriceAccuracyValid(messageWrapper, order)})

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun getAssetPair(order: MarketOrder) = assetsPairsHolder.getAssetPair(order.assetPairId)

    private fun orderInfo(order: MarketOrder) = """market order id: ${order.id}}, client: ${order.clientId},
        |asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)},
        |straight: ${order.straight}""".trimMargin()

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