package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.Date

@Service
class LimitOrderMassCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                                  private val cancellerFactory: GenericLimitOrdersCancellerFactory) : AbstractService {
    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val orders = getOrders(messageWrapper)

        if (orders.isNotEmpty()) {
            cancelOrders(orders, now, messageWrapper)
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    private fun getOrders(messageWrapper: MessageWrapper): List<LimitOrder> {
        val message = getMessage(messageWrapper)

        val clientId = message.clientId
        val assetPairId = if (message.hasAssetPairId()) message.assetPairId else null
        val isBuy = if (message.hasIsBuy()) message.isBuy else null

        LOGGER.debug("Got mass limit order cancel request id: ${message.uid}, clientId: $clientId, assetPairId: $assetPairId, isBuy: $isBuy")

        return genericLimitOrderService.searchOrders(clientId, assetPairId, isBuy)
    }

    private fun cancelOrders(orders: List<LimitOrder>, now: Date,
                             messageWrapper: MessageWrapper) {
        val canceller = cancellerFactory.create(LOGGER, now)

        canceller.preProcessLimitOrders(orders)
                .applyFull(messageWrapper.id!!,
                        messageWrapper.messageId!!,
                        messageWrapper.processedMessage(),
                        MessageType.valueOf(messageWrapper.type) ?: throw Exception("Unknown message type ${messageWrapper.type}"),
                        false)
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderMassCancel

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = ProtocolMessages.LimitOrderMassCancel.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
        messageWrapper.id = message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

}