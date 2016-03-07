package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger

class LimitOrderCancelService(private val limitOrderService: LimitOrderService): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got limit order cancel request id: ${message.uid}")

        limitOrderService.cancelLimitOrder(message.limitOrderId.toString())
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrderCancel {
        return ProtocolMessages.LimitOrderCancel.parseFrom(array)
    }
}
