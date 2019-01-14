package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.context.MultilimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.MultilimitOrderParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class MultilimitOrderContextParser(
        @Qualifier("multiLimitOrderPreProcessingLogger")
        val logger: ThrottlingLogger,
        val applicationSettingsHolder: ApplicationSettingsHolder,
        val assertPairsHolder: AssetsPairsHolder,
        val assetsHolder: AssetsHolder) : ContextParser<MultilimitOrderParsedData> {
    override fun parse(messageWrapper: MessageWrapper): MultilimitOrderParsedData {
        val message = parseMultiLimitOrder(messageWrapper.byteArray)
        val trustedClient = applicationSettingsHolder.isTrustedClient(message.clientId)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.context = getContext(messageWrapper.messageId!!, trustedClient, message)
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = if (trustedClient) {
            null
        } else {
            ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
        }

        return MultilimitOrderParsedData(messageWrapper)
    }

    private fun parseMultiLimitOrder(array: ByteArray): ProtocolMessages.MultiLimitOrder {
        return ProtocolMessages.MultiLimitOrder.parseFrom(array)
    }

    private fun getContext(messageId: String,
                           trustedClient: Boolean,
                           message: ProtocolMessages.MultiLimitOrder): MultilimitOrderContext {
        val assetPair = assertPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        val baseAsset = assetPair?.let {
            assetsHolder.getAssetAllowNulls(it.baseAssetId)
        }

        val quotingAsset = assetPair?.let {
            assetsHolder.getAssetAllowNulls(it.quotingAssetId)
        }

        return MultilimitOrderContext(assetPair,
                baseAsset,
                quotingAsset,
                message.clientId,
                trustedClient,
                readMultiLimitOrder(messageId, message, trustedClient))
    }

    private fun readMultiLimitOrder(messageId: String,
                                    message: ProtocolMessages.MultiLimitOrder,
                                    isTrustedClient: Boolean): MultiLimitOrder {
        logger.debug("Got ${if (!isTrustedClient) "client " else ""}multi limit order id: ${message.uid}, " +
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
        val orders = ArrayList<LimitOrder>()
        message.ordersList.forEach { currentOrder ->
            if (!isTrustedClient) {
                logger.debug("Incoming limit order (message id: $messageId): ${getIncomingOrderInfo(currentOrder)}")
            }
            val type = if (currentOrder.hasType()) LimitOrderType.getByExternalId(currentOrder.type) else LimitOrderType.LIMIT
            val status = when (type) {
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
                    previousExternalId = previousExternalId,
                    timeInForce = if (currentOrder.hasTimeInForce()) OrderTimeInForce.getByExternalId(currentOrder.timeInForce) else null,
                    expiryTime = if (currentOrder.hasExpiryTime()) Date(currentOrder.expiryTime) else null,
                    parentOrderExternalId = null,
                    childOrderExternalId = null
            )

            orders.add(order)
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
                orders,
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
}