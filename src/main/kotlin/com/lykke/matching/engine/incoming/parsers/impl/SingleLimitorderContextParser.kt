package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class SingleLimitorderContextParser: ContextParser {
    override fun parse(messageWrapper: MessageWrapper): MessageWrapper {
        val orderProcessingStartTime = Date()
        val builder = parseMessage(messageWrapper, orderProcessingStartTime)

        val context = builder.build()

        messageWrapper.context = context
        messageWrapper.id = context.id
        messageWrapper.messageId = context.messageId

        return messageWrapper
    }


    fun parseMessage(messageWrapper: MessageWrapper, orderProcessingStartTime: Date): SingleLimitContext.Builder {
        val builder = SingleLimitContext.Builder()
        val order: LimitOrder
        val cancelOrders: Boolean

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage =  parseOldLimitOrder(messageWrapper.byteArray)
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, BigDecimal.valueOf(oldMessage.volume),
                    BigDecimal.valueOf(oldMessage.price), OrderStatus.InOrderBook.name, orderProcessingStartTime, Date(oldMessage.timestamp), orderProcessingStartTime, BigDecimal.valueOf(oldMessage.volume), null,
                    type = LimitOrderType.LIMIT, lowerLimitPrice = null, lowerPrice = null, upperLimitPrice = null, upperPrice = null, previousExternalId = null)

            SingleLimitOrderService.LOGGER.info("Got old limit order messageId: ${messageWrapper.messageId} id: ${oldMessage.uid}, client ${oldMessage.clientId}, " +
                    "assetPair: ${oldMessage.assetPairId}, " +
                    "volume: ${NumberUtils.roundForPrint(oldMessage.volume)}, price: ${NumberUtils.roundForPrint(oldMessage.price)}, " +
                    "cancel: ${oldMessage.cancelAllPreviousLimitOrders}")

            cancelOrders = oldMessage.cancelAllPreviousLimitOrders
            builder.id = oldMessage.uid.toString()
            builder.messageId = if (oldMessage.hasMessageId()) oldMessage.messageId else oldMessage.uid.toString()
        } else {
            val message = parseLimitOrder(messageWrapper.byteArray)
            builder.id = message.uid
            builder.messageId = if (message.hasMessageId()) message.messageId else message.uid
            order = createOrder(message, orderProcessingStartTime)
            SingleLimitOrderService.LOGGER.info("Got limit order ${incomingMessageInfo(messageWrapper.messageId, message, order)}")

            cancelOrders = message.cancelAllPreviousLimitOrders
        }

        return builder
                .limitOrder(order)
                .orderProcessingStartTime(orderProcessingStartTime)
                .cancelOrders(cancelOrders)
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }

    private fun createOrder(message: ProtocolMessages.LimitOrder, now: Date): LimitOrder {
        val type = if (message.hasType()) LimitOrderType.getByExternalId(message.type) else LimitOrderType.LIMIT
        val status = when(type) {
            LimitOrderType.LIMIT -> OrderStatus.InOrderBook
            LimitOrderType.STOP_LIMIT -> OrderStatus.Pending
        }
        val feeInstruction = if (message.hasFee()) LimitOrderFeeInstruction.create(message.fee) else null
        val feeInstructions = NewLimitOrderFeeInstruction.create(message.feesList)
        return LimitOrder(UUID.randomUUID().toString(),
                message.uid,
                message.assetPairId,
                message.clientId,
                BigDecimal.valueOf(message.volume),
                if (message.hasPrice()) BigDecimal.valueOf(message.price) else BigDecimal.ZERO,
                status.name,
                now,
                Date(message.timestamp),
                now,
                BigDecimal.valueOf(message.volume),
                null,
                fee = feeInstruction,
                fees = listOfLimitOrderFee(feeInstruction, feeInstructions),
                type = type,
                lowerLimitPrice = if (message.hasLowerLimitPrice()) BigDecimal.valueOf(message.lowerLimitPrice )else null,
                lowerPrice = if (message.hasLowerPrice()) BigDecimal.valueOf(message.lowerPrice) else null,
                upperLimitPrice = if (message.hasUpperLimitPrice()) BigDecimal.valueOf(message.upperLimitPrice) else null,
                upperPrice = if (message.hasUpperPrice()) BigDecimal.valueOf(message.upperPrice) else null,
                previousExternalId = null)
    }

    private fun incomingMessageInfo(messageId: String?, message: ProtocolMessages.LimitOrder, order: LimitOrder): String {
        return "id: ${message.uid}" +
                ", messageId $messageId" +
                ", type: ${order.type}" +
                ", client: ${message.clientId}" +
                ", assetPair: ${message.assetPairId}" +
                ", volume: ${NumberUtils.roundForPrint(message.volume)}" +
                ", price: ${NumberUtils.roundForPrint(order.price)}" +
                (if (order.lowerLimitPrice != null) ", lowerLimitPrice: ${NumberUtils.roundForPrint(order.lowerLimitPrice)}" else "") +
                (if (order.lowerPrice != null) ", lowerPrice: ${NumberUtils.roundForPrint(order.lowerPrice)}" else "") +
                (if (order.upperLimitPrice != null) ", upperLimitPrice: ${NumberUtils.roundForPrint(order.upperLimitPrice)}" else "") +
                (if (order.upperPrice != null) ", upperPrice: ${NumberUtils.roundForPrint(order.upperPrice)}" else "") +
                ", cancel: ${message.cancelAllPreviousLimitOrders}" +
                ", fee: ${order.fee}" +
                ", fees: ${order.fees}"
    }
}