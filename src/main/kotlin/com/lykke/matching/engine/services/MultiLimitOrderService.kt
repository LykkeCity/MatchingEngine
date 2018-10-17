package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Date
import java.util.UUID

@Service
class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                             private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                             private val assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val balancesHolder: BalancesHolder,
                             genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory? = null,
                             feeProcessor: FeeProcessor) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
    }

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, feeProcessor)
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        processMultiOrder(messageWrapper)
    }

    private fun processMultiOrder(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrder
        val isTrustedClient = balancesHolder.isTrustedClient(message.clientId)

        LOGGER.debug("Got ${if (!isTrustedClient) "client " else ""}multi limit order id: ${message.uid}, " +
                (if (messageWrapper.messageId != message.uid) "messageId: ${messageWrapper.messageId}, " else "") +
                "client ${message.clientId}, " +
                "assetPair: ${message.assetPairId}, " +
                (if (message.hasCancelAllPreviousLimitOrders()) "cancelPrevious: ${message.cancelAllPreviousLimitOrders}, " else "") +
                (if (message.hasCancelMode()) "cancelMode: ${message.cancelMode}" else ""))

        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        if (assetPair == null) {
            LOGGER.info("Unable to process message (${messageWrapper.messageId}): unknown asset pair")
            writeResponse(messageWrapper, MessageStatus.UNKNOWN_ASSET)
            return
        }
        val multiLimitOrder = readMultiLimitOrder(message, isTrustedClient, assetPair)
        val now = Date()

        var buySideOrderBookChanged = false
        var sellSideOrderBookChanged = false

        var previousBuyOrders: Collection<LimitOrder>? = null
        var previousSellOrders: Collection<LimitOrder>? = null
        val ordersToReplace = mutableListOf<LimitOrder>()

        val ordersToCancel = ArrayList<LimitOrder>()
        if (multiLimitOrder.cancelAllPreviousLimitOrders) {
            if (multiLimitOrder.cancelBuySide) {
                previousBuyOrders = limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, true)
                ordersToCancel.addAll(previousBuyOrders)
                buySideOrderBookChanged = true
            }
            if (multiLimitOrder.cancelSellSide) {
                previousSellOrders = limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, false)
                ordersToCancel.addAll(previousSellOrders)
                sellSideOrderBookChanged = true
            }
        }

        val notFoundReplacements = mutableMapOf<String, LimitOrder>()

        buySideOrderBookChanged = processReplacements(multiLimitOrder,
                true,
                notFoundReplacements,
                previousBuyOrders,
                ordersToCancel,
                ordersToReplace) || buySideOrderBookChanged

        sellSideOrderBookChanged = processReplacements(multiLimitOrder,
                false,
                notFoundReplacements,
                previousSellOrders,
                ordersToCancel,
                ordersToReplace) || sellSideOrderBookChanged

        val cancelResult = genericLimitOrdersCancellerFactory.create(LOGGER, now)
                .preProcessLimitOrders(ordersToCancel)
                .processLimitOrders()

        val orderBook = cancelResult.assetOrderBooks[multiLimitOrder.assetPairId] ?: limitOrderService.getOrderBook(multiLimitOrder.assetPairId).copy()

        val cancelBaseVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.baseAssetId }
                .map { t -> -t.reservedAmount }
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        val cancelQuotingVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.quotingAssetId }
                .map { t -> -t.reservedAmount }
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        notFoundReplacements.values.forEach {
            it.updateStatus(OrderStatus.NotFoundPrevious, now)
        }
        ordersToReplace.forEach {
            LOGGER.info("Order (${it.externalId}) is replaced by (${(multiLimitOrder.buyReplacements[it.externalId]
                    ?: multiLimitOrder.sellReplacements[it.externalId])?.externalId})")
            it.updateStatus(OrderStatus.Replaced, now)
        }

        val processor = limitOrdersProcessorFactory.create(matchingEngine,
                now,
                balancesHolder.isTrustedClient(multiLimitOrder.clientId),
                multiLimitOrder.clientId,
                assetPair,
                assetsHolder.getAsset(assetPair.baseAssetId),
                assetsHolder.getAsset(assetPair.quotingAssetId),
                orderBook,
                cancelBaseVolume,
                cancelQuotingVolume,
                ordersToCancel,
                cancelResult.clientsOrdersWithTrades,
                cancelResult.trustedClientsOrdersWithTrades,
                LOGGER)

        matchingEngine.initTransaction()
        val result = processor.preProcess(messageWrapper.messageId!!, multiLimitOrder.orders)
                .apply(messageWrapper.messageId!!,
                        messageWrapper.processedMessage,
                        multiLimitOrder.messageUid, MessageType.MULTI_LIMIT_ORDER,
                        buySideOrderBookChanged, sellSideOrderBookChanged)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = result.success
        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()

        if (!result.success) {
            val errorMessage = "Unable to save result data"
            LOGGER.error("$errorMessage (multi limit order id ${multiLimitOrder.messageUid})")

            messageWrapper.writeMultiLimitOrderResponse(responseBuilder
                    .setStatus(MessageStatus.RUNTIME.type)
                    .setAssetPairId(multiLimitOrder.assetPairId)
                    .setStatusReason(errorMessage))

            return
        }

        responseBuilder.setId(multiLimitOrder.messageUid)
                .setStatus(MessageStatus.OK.type).assetPairId = multiLimitOrder.assetPairId

        result.orders.forEach { processedOrder ->
            val order = processedOrder.order
            val statusBuilder = ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder()
                    .setId(order.externalId)
                    .setMatchingEngineId(order.id)
                    .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                    .setVolume(order.volume.toDouble())
                    .setPrice(order.price.toDouble())
            processedOrder.reason?.let { statusBuilder.statusReason = processedOrder.reason }
            responseBuilder.addStatuses(statusBuilder.build())
        }
        messageWrapper.writeMultiLimitOrderResponse(responseBuilder)

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair, now)
    }

    private fun readMultiLimitOrder(message: ProtocolMessages.MultiLimitOrder,
                                    isTrustedClient: Boolean,
                                    assetPair: AssetPair): MultiLimitOrder {
        val clientId = message.clientId
        val messageUid = message.uid
        val assetPairId = message.assetPairId
        val cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
        val cancelMode = if (message.hasCancelMode()) OrderCancelMode.getByExternalId(message.cancelMode) else OrderCancelMode.NOT_EMPTY_SIDE
        val now = Date()
        var cancelBuySide = cancelMode == OrderCancelMode.BUY_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES
        var cancelSellSide = cancelMode == OrderCancelMode.SELL_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES

        val buyReplacements = mutableMapOf<String, LimitOrder>()
        val sellReplacements = mutableMapOf<String, LimitOrder>()

        val baseAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)
        val quotingAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)

        val filter = MultiOrderFilter(isTrustedClient,
                baseAssetAvailableBalance,
                quotingAssetAvailableBalance,
                assetsHolder.getAsset(assetPair.quotingAssetId).accuracy,
                now,
                message.ordersList.size,
                LOGGER)

        message.ordersList.forEach { currentOrder ->

            val feeInstruction = if (currentOrder.hasFee()) LimitOrderFeeInstruction.create(currentOrder.fee) else null
            val feeInstructions = NewLimitOrderFeeInstruction.create(currentOrder.feesList)
            val previousExternalId = if (currentOrder.hasOldUid()) currentOrder.oldUid else null

            val order = LimitOrder(UUID.randomUUID().toString(),
                    currentOrder.uid,
                    message.assetPairId,
                    message.clientId,
                    BigDecimal.valueOf(currentOrder.volume),
                    BigDecimal.valueOf(currentOrder.price),
                    OrderStatus.InOrderBook.name,
                    now,
                    Date(message.timestamp),
                    now,
                    BigDecimal.valueOf(currentOrder.volume),
                    null,
                    fee = feeInstruction,
                    fees = listOfLimitOrderFee(feeInstruction, feeInstructions),
                    type = LimitOrderType.LIMIT,
                    upperPrice = null,
                    upperLimitPrice = null,
                    lowerPrice = null,
                    lowerLimitPrice = null,
                    previousExternalId = previousExternalId)

            filter.checkAndAdd(order)
            previousExternalId?.let {
                (if (order.isBuySide()) buyReplacements else sellReplacements)[it] = order
            }

            if (cancelAllPreviousLimitOrders && cancelMode == OrderCancelMode.NOT_EMPTY_SIDE) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        return MultiLimitOrder(messageUid,
                clientId,
                assetPairId,
                filter.getResult(),
                cancelAllPreviousLimitOrders,
                cancelBuySide,
                cancelSellSide,
                cancelMode,
                buyReplacements,
                sellReplacements)
    }

    private fun processReplacements(multiLimitOrder: MultiLimitOrder,
                                    isBuy: Boolean,
                                    notFoundReplacements: MutableMap<String, LimitOrder>,
                                    previousOrders: Collection<LimitOrder>?,
                                    ordersToCancel: MutableCollection<LimitOrder>,
                                    ordersToReplace: MutableCollection<LimitOrder>): Boolean {
        var addedToCancel = false
        val replacements = if (isBuy) multiLimitOrder.buyReplacements else multiLimitOrder.sellReplacements
        if (replacements.isEmpty()) {
            return addedToCancel
        }
        val mutableReplacements = replacements.toMutableMap()
        val isAlreadyCancelled = isBuy && multiLimitOrder.cancelBuySide || !isBuy && multiLimitOrder.cancelSellSide
        val ordersToCheck = previousOrders ?: limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, isBuy)
        ordersToCheck.forEach {
            if (mutableReplacements.containsKey(it.externalId)) {
                val newOrder = mutableReplacements.remove(it.externalId)
                if (!isAlreadyCancelled) {
                    ordersToCancel.add(it)
                    addedToCancel = true
                }
                if (newOrder?.status == OrderStatus.InOrderBook.name) {
                    ordersToReplace.add(it)
                }
            }
        }
        notFoundReplacements.putAll(mutableReplacements)
        return addedToCancel
    }

    private fun parseMultiLimitOrder(array: ByteArray): ProtocolMessages.MultiLimitOrder {
        return ProtocolMessages.MultiLimitOrder.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parseMultiLimitOrder(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = if (settings.isTrustedClient(message.clientId))
            null
        else
            ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val assetPairId = (messageWrapper.parsedMessage as ProtocolMessages.MultiLimitOrder).assetPairId
        messageWrapper.writeMultiLimitOrderResponse(ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(status.type).setAssetPairId(assetPairId))
    }
}