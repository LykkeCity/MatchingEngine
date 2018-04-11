package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.LimitOrdersCancellerFactory
import org.apache.log4j.Logger
import java.util.Date

class MultiLimitOrderCancelService(private val limitOrderService: GenericLimitOrderService,
                                   private val limitOrdersCancellerFactory: LimitOrdersCancellerFactory): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrderCancel
        LOGGER.debug("Got multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}")
        val now = Date()
        val ordersToCancel = limitOrderService.getAllPreviousOrders(message.clientId, message.assetPairId, message.isBuy)
        if (ordersToCancel.isNotEmpty()) {
            limitOrdersCancellerFactory.create(now)
                    .preProcess(ordersToCancel)
                    .applyFull(message.uid, MessageType.MULTI_LIMIT_ORDER_CANCEL.name, false)
        }
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
        LOGGER.debug("Multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.MultiLimitOrderCancel {
        return ProtocolMessages.MultiLimitOrderCancel.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.uid
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId).setStatus(status.type).build())
    }
}
