package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.UUID

class SingleLimitOrderService(genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory.create(LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val order: LimitOrder
        val now = Date()
        val isCancelOrders: Boolean

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrder
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, BigDecimal.valueOf(oldMessage.volume),
                    BigDecimal.valueOf(oldMessage.price), OrderStatus.InOrderBook.name, now, Date(oldMessage.timestamp), now, BigDecimal.valueOf(oldMessage.volume), null,
                    type = LimitOrderType.LIMIT, lowerLimitPrice = null, lowerPrice = null, upperLimitPrice = null, upperPrice = null, previousExternalId = null)

            LOGGER.info("Got old limit order messageId: ${messageWrapper.messageId} id: ${oldMessage.uid}, client ${oldMessage.clientId}, " +
                    "assetPair: ${oldMessage.assetPairId}, " +
                    "volume: ${NumberUtils.roundForPrint(oldMessage.volume)}, price: ${NumberUtils.roundForPrint(oldMessage.price)}, " +
                    "cancel: ${oldMessage.cancelAllPreviousLimitOrders}")

            isCancelOrders = oldMessage.cancelAllPreviousLimitOrders
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrder
            order = createOrder(message, now)
            LOGGER.info("Got limit order ${incomingMessageInfo(messageWrapper.messageId, message, order)}")
            isCancelOrders = message.cancelAllPreviousLimitOrders
        }

        genericLimitOrderProcessor.processOrder(messageWrapper, order, isCancelOrders, now)

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
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

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val message =  parseOldLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.id = message.uid.toString()
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
            messageWrapper.timestamp = message.timestamp
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type))
        }
    }
}

