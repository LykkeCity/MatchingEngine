package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*
import java.util.stream.Collectors

@Service
class LimitOrderCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val cancellerFactory: GenericLimitOrdersCancellerFactory) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            processOldLimitOrderCancelMessage(messageWrapper)
        }

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderCancel
        val orderIds = message.limitOrderIdList.toSet()
        LOGGER.debug("Got limit order cancel request (id: ${message.uid}, orders: $orderIds)")

        if (orderIds.isEmpty()) {
            val errorMessage = "Orders ids list is empty (request id: ${message.uid})"
            LOGGER.info(errorMessage)
            writeResponse(messageWrapper, MessageStatus.BAD_REQUEST, errorMessage)
            return
        }

        val typeToOrder = getOrders(orderIds)

        if (typeToOrder.isEmpty()) {
            LOGGER.info("Unable to find order ids: $orderIds")
            writeResponse(messageWrapper, MessageStatus.LIMIT_ORDER_NOT_FOUND)
            return
        }

        val updateSuccessful = cancelOrders(typeToOrder[LimitOrderType.LIMIT], typeToOrder[LimitOrderType.STOP_LIMIT], now, messageWrapper)

        if (!updateSuccessful) {
            writeResponse(messageWrapper, MessageStatus.RUNTIME, "Unable to save result")
            return
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    private fun cancelOrders(limitOrders: List<LimitOrder>?, stopOrders: List<LimitOrder>?, now: Date, messageWrapper: MessageWrapper): Boolean {
        val canceller = cancellerFactory.create(LOGGER, now)

        canceller.preProcessLimitOrders(limitOrders ?: emptyList())
        canceller.preProcessStopLimitOrders(stopOrders ?: emptyList())

        return canceller.applyFull(messageWrapper.id!!,
                messageWrapper.messageId!!,
                messageWrapper.processedMessage(),
                MessageType.valueOf(messageWrapper.type) ?: throw Exception("Unknown message type ${messageWrapper.type}"),
                false)
    }

    private fun getOrders(orderIds: Set<String>): Map<LimitOrderType, List<LimitOrder>> {
        return orderIds.stream()
                .map(::getOrder)
                .filter { limitOrder -> limitOrder != null }
                .map { t -> t!! }
                .collect(Collectors.groupingBy({ limitOrder: LimitOrder -> limitOrder.type!! }))
    }

    private fun getOrder(orderId: String): LimitOrder? {
        return genericLimitOrderService.getOrder(orderId) ?: genericStopLimitOrderService.getOrder(orderId)
    }

    private fun processOldLimitOrderCancelMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrderCancel
        LOGGER.debug("Got old limit  order messageId: ${messageWrapper.messageId}  (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

        genericLimitOrderService.cancelLimitOrder(Date(), message.limitOrderId.toString(), true)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
    }

    private fun parseOldLimitOrderCancel(array: ByteArray): ProtocolMessages.OldLimitOrderCancel {
        return ProtocolMessages.OldLimitOrderCancel.parseFrom(array)
    }

    private fun parseLimitOrderCancel(array: ByteArray): ProtocolMessages.LimitOrderCancel {
        return ProtocolMessages.LimitOrderCancel.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message = parseOldLimitOrderCancel(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message = parseLimitOrderCancel(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }
    }

    private fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return
        }

        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)

        message?.let {
            builder.statusReason = message
        }

        messageWrapper.writeNewResponse(builder)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        writeResponse(messageWrapper, status, null)
    }
}
