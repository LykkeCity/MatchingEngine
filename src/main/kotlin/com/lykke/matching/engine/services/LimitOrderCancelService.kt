package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList

class LimitOrderCancelService(genericLimitOrderService: GenericLimitOrderService,
                              genericStopLimitOrderService: GenericStopLimitOrderService,
                              cancellerFactory: GenericLimitOrdersCancellerFactory) :
        AbstractLimitOrdersCancelService(genericLimitOrderService, genericStopLimitOrderService, cancellerFactory) {

    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun getOrders(messageWrapper: MessageWrapper): Orders {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrderCancel
            LOGGER.debug("Got old limit order (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

            genericLimitOrderService.cancelLimitOrder(message.limitOrderId.toString(), true)
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return Orders.processed()
        }

        val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderCancel
        val orderIds = message.limitOrderIdList.toList()
        LOGGER.debug("Got limit order cancel request (id: ${message.uid}, orders: $orderIds)")

        if (orderIds.isEmpty()) {
            val errorMessage = "Orders ids list is empty (request id: ${message.uid})"
            LOGGER.info(errorMessage)
            writeResponse(messageWrapper, MessageStatus.BAD_REQUEST, errorMessage)
            return Orders.processed()
        }

        val orders = LinkedList<NewLimitOrder>()
        val stopOrders = LinkedList<NewLimitOrder>()
        val notFoundOrderIds = LinkedList<String>()
        orderIds.forEach { orderId ->
            var isStopOrder = false
            var order = genericLimitOrderService.getOrder(orderId)
            if (order == null) {
                order = genericStopLimitOrderService.getOrder(orderId)
                isStopOrder = true
            }
            if (order == null) {
                notFoundOrderIds.add(orderId)
            } else {
                LOGGER.info("Unable to find order id: ${message.limitOrderId}")
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setStatus(MessageStatus.LIMIT_ORDER_NOT_FOUND.type))
            }
        }

        if (orders.isEmpty() && stopOrders.isEmpty()) {
            LOGGER.info("Unable to find order ids: $notFoundOrderIds")
            writeResponse(messageWrapper, MessageStatus.LIMIT_ORDER_NOT_FOUND)
            return Orders.processed()
        }

        return Orders.notProcessed(orders, stopOrders)
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
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message = parseLimitOrderCancel(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }
        LOGGER.info("Parsed ${LimitOrderCancelService::class.java.name} message with messageId ${messageWrapper.messageId}")
    }

    private fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            val builder = ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type)
            message?.let {
                builder.statusReason = message
            }
            messageWrapper.writeNewResponse(builder)
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        writeResponse(messageWrapper, status, null)
    }
}
