package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.UUID

@Service
class MultiLimitOrderService(private val executionContextFactory: ExecutionContextFactory,
                             private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                             private val stopOrderBookProcessor: StopOrderBookProcessor,
                             private val executionDataApplyService: ExecutionDataApplyService,
                             private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                             private val assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val balancesHolder: BalancesHolder,
                             private val applicationSettingsHolder: ApplicationSettingsHolder,
                             private val messageProcessingStatusHolder: MessageProcessingStatusHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        processMultiOrder(messageWrapper)
    }

    private fun processMultiOrder(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrder
        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        if (assetPair == null) {
            LOGGER.info("Unable to process message (${messageWrapper.messageId}): unknown asset pair ${message.assetPairId}")
            writeResponse(messageWrapper, MessageStatus.UNKNOWN_ASSET)
            return
        }

        if (messageProcessingStatusHolder.isTradeDisabled(assetPair)) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        val isTrustedClient = applicationSettingsHolder.isTrustedClient(message.clientId)

        val multiLimitOrder = readMultiLimitOrder(messageWrapper.messageId!!, message, isTrustedClient, assetPair)
        val now = Date()

        val executionContext = executionContextFactory.create(messageWrapper.messageId!!,
                messageWrapper.id!!,
                MessageType.MULTI_LIMIT_ORDER,
                messageWrapper.processedMessage,
                mapOf(Pair(assetPair.assetPairId, assetPair)),
                now,
                LOGGER)

        previousLimitOrdersProcessor.cancelAndReplaceOrders(multiLimitOrder.clientId,
                multiLimitOrder.assetPairId,
                multiLimitOrder.cancelAllPreviousLimitOrders,
                multiLimitOrder.cancelBuySide,
                multiLimitOrder.cancelSellSide,
                multiLimitOrder.buyReplacements,
                multiLimitOrder.sellReplacements,
                executionContext)

        val processedOrders = genericLimitOrdersProcessor.processOrders(multiLimitOrder.orders, executionContext)
        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)

        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
        if (!persisted) {
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

        processedOrders.forEach { processedOrder ->
            val order = processedOrder.order
            val statusBuilder = ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder()
                    .setId(order.externalId)
                    .setMatchingEngineId(order.id)
                    .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                    .setVolume(order.volume.toDouble())
                    .setPrice(order.price.toDouble())
            processedOrder.reason?.let { statusBuilder.statusReason = processedOrder.reason }
            responseBuilder.addStatuses(statusBuilder)
        }
        writeResponse(messageWrapper, responseBuilder)

    }

    private fun readMultiLimitOrder(messageId: String,
                                    message: ProtocolMessages.MultiLimitOrder,
                                    isTrustedClient: Boolean,
                                    assetPair: AssetPair): MultiLimitOrder {
        LOGGER.debug("Got ${if (!isTrustedClient) "client " else ""}multi limit order id: ${message.uid}, " +
                (if (messageId != message.uid) "messageId: $messageId, " else "") +
                "client ${message.clientId}, " +
                "assetPair: ${message.assetPairId}, " +
                "ordersCount: ${message.ordersCount}, " +
                (if (message.hasCancelAllPreviousLimitOrders()) "cancelPrevious: ${message.cancelAllPreviousLimitOrders}, " else "") +
                (if (message.hasCancelMode()) "cancelMode: ${message.cancelMode}" else ""))

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
            if (!isTrustedClient) {
                LOGGER.debug("Incoming limit order (message id: $messageId): ${getIncomingOrderInfo(currentOrder)}")
            }
            val type = if (currentOrder.hasType()) LimitOrderType.getByExternalId(currentOrder.type) else LimitOrderType.LIMIT
            val status = when(type) {
                LimitOrderType.LIMIT -> OrderStatus.InOrderBook
                LimitOrderType.STOP_LIMIT -> OrderStatus.Pending
            }
            val price = if (currentOrder.hasPrice()) BigDecimal.valueOf(currentOrder.price) else BigDecimal.ZERO
            val lowerLimitPrice = if (currentOrder.hasLowerLimitPrice()) BigDecimal.valueOf(currentOrder.lowerLimitPrice) else null
            val lowerPrice = if (currentOrder.hasLowerPrice()) BigDecimal.valueOf(currentOrder.lowerPrice) else null
            val upperLimitPrice = if (currentOrder.hasUpperLimitPrice()) BigDecimal.valueOf(currentOrder.upperLimitPrice) else null
            val upperPrice = if (currentOrder.hasUpperPrice()) BigDecimal.valueOf(currentOrder.upperPrice) else null
            val feeInstruction = if (currentOrder.hasFee()) LimitOrderFeeInstruction.create(currentOrder.fee) else null
            val feeInstructions = NewLimitOrderFeeInstruction.create(currentOrder.feesList)
            val previousExternalId = if (currentOrder.hasOldUid()) currentOrder.oldUid else null

            val order = LimitOrder(UUID.randomUUID().toString(),
                    currentOrder.uid,
                    message.assetPairId,
                    message.clientId,
                    BigDecimal.valueOf(currentOrder.volume),
                    price,
                    status.name,
                    now,
                    Date(message.timestamp),
                    now,
                    BigDecimal.valueOf(currentOrder.volume),
                    null,
                    fee = feeInstruction,
                    fees = listOfLimitOrderFee(feeInstruction, feeInstructions),
                    type = type,
                    lowerLimitPrice = lowerLimitPrice,
                    lowerPrice = lowerPrice,
                    upperLimitPrice = upperLimitPrice,
                    upperPrice = upperPrice,
                    previousExternalId = previousExternalId
//                    timeInForce = if (currentOrder.hasTimeInForce()) OrderTimeInForce.getByExternalId(currentOrder.timeInForce) else null,
//                    expiryTime = if (currentOrder.hasExpiryTime()) Date(currentOrder.expiryTime) else null
            )

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

    private fun getIncomingOrderInfo(incomingOrder: ProtocolMessages.MultiLimitOrder.Order): String {
        return "id: ${incomingOrder.uid}" +
                (if (incomingOrder.hasType()) ", type: ${incomingOrder.type}" else "") +
                ", volume: ${NumberUtils.roundForPrint(incomingOrder.volume)}" +
                (if (incomingOrder.hasPrice()) ", price: ${NumberUtils.roundForPrint(incomingOrder.price)}" else "") +
                (if (incomingOrder.hasLowerLimitPrice()) ", lowerLimitPrice: ${NumberUtils.roundForPrint(incomingOrder.lowerLimitPrice)}" else "") +
                (if (incomingOrder.hasLowerPrice()) ", lowerPrice: ${NumberUtils.roundForPrint(incomingOrder.lowerPrice)}" else "") +
                (if (incomingOrder.hasUpperLimitPrice()) ", upperLimitPrice: ${NumberUtils.roundForPrint(incomingOrder.upperLimitPrice)}" else "") +
                (if (incomingOrder.hasUpperPrice()) ", upperPrice: ${NumberUtils.roundForPrint(incomingOrder.upperPrice)}" else "") +
                (if (incomingOrder.hasOldUid()) ", oldUid: ${incomingOrder.oldUid}" else "") +
//                (if (incomingOrder.hasTimeInForce()) ", timeInForce: ${incomingOrder.timeInForce}" else "") +
//                (if (incomingOrder.hasExpiryTime()) ", expiryTime: ${incomingOrder.expiryTime}" else "") +
                (if (incomingOrder.hasFee()) ", fee: ${getIncomingFeeInfo(incomingOrder.fee)}" else "") +
                (if (incomingOrder.feesCount > 0) ", fees: ${incomingOrder.feesList.asSequence().map { getIncomingFeeInfo(incomingOrder.fee) }.joinToString(", ")}" else "")
    }

    private fun getIncomingFeeInfo(incomingFee: ProtocolMessages.LimitOrderFee): String {
        return "type: ${incomingFee.type}, " +
                (if (incomingFee.hasMakerSize()) ", makerSize: ${NumberUtils.roundForPrint(incomingFee.makerSize)}" else "") +
                (if (incomingFee.hasTakerSize()) ", takerSize: ${NumberUtils.roundForPrint(incomingFee.takerSize)}" else "") +
                (if (incomingFee.hasSourceClientId()) ", sourceClientId: ${incomingFee.sourceClientId}" else "") +
                (if (incomingFee.hasTargetClientId()) ", targetClientId: ${incomingFee.targetClientId}" else "") +
                (if (incomingFee.hasMakerSizeType()) ", makerSizeType: ${incomingFee.makerSizeType}" else "") +
                (if (incomingFee.hasTakerSizeType()) ", takerSizeType: ${incomingFee.takerSizeType}" else "") +
                (if (incomingFee.hasMakerFeeModificator()) ", makerFeeModificator: ${NumberUtils.roundForPrint(incomingFee.makerFeeModificator)}" else "") +
                (if (incomingFee.assetIdCount > 0) ", assetIds: ${incomingFee.assetIdList}}" else "")
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
        messageWrapper.processedMessage = if (applicationSettingsHolder.isTrustedClient(message.clientId))
            null
        else
            ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
    }

    fun writeResponse(messageWrapper: MessageWrapper, responseBuilder: ProtocolMessages.MultiLimitOrderResponse.Builder) {
        messageWrapper.writeMultiLimitOrderResponse(responseBuilder)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val assetPairId = (messageWrapper.parsedMessage as ProtocolMessages.MultiLimitOrder).assetPairId
        messageWrapper.writeMultiLimitOrderResponse(ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(status.type).setAssetPairId(assetPairId))
    }
}