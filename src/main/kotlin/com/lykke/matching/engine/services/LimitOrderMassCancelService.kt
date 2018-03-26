package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.LimitOrdersCancellerFactory
import org.apache.log4j.Logger
import java.util.Date

class LimitOrderMassCancelService(genericLimitOrderService: GenericLimitOrderService,
                                  cancellerFactory: LimitOrdersCancellerFactory) :
        AbstractLimitOrdersCancelService(genericLimitOrderService, cancellerFactory) {

    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun getOrders(messageWrapper: MessageWrapper): Orders {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderMassCancel

        val clientId = message.clientId
        val assetPairId = if (message.hasAssetPairId()) message.assetPairId else null
        val isBuy = if (message.hasIsBuy()) message.isBuy else null

        LOGGER.debug("Got mass limit order cancel request id: ${message.uid}, clientId: $clientId, assetPairId: $assetPairId, isBuy: $isBuy")

        return Orders.notProcessed(genericLimitOrderService.searchOrders(clientId, assetPairId, isBuy))
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = ProtocolMessages.LimitOrderMassCancel.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderMassCancel
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(status.type).build())
    }

}