package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.util.*

@Component
class LimitOrdersCancelHelper(private val cancellerFactory: GenericLimitOrdersCancellerFactory) {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    class CancelRequest(val uid: String,
                        val messageId: String,
                        val messageType: MessageType,
                        val limitOrders: List<LimitOrder>?,
                        val stopOrders: List<LimitOrder>?,
                        val now: Date,
                        val processedMessage: ProcessedMessage,
                        val validateBalances: Boolean)

    fun cancelOrders(cancelRequest: CancelRequest): Boolean {
        val canceller = cancellerFactory.create(LOGGER, cancelRequest.now)

        canceller.preProcessLimitOrders(cancelRequest.limitOrders ?: emptyList())
        canceller.preProcessStopLimitOrders(cancelRequest.stopOrders ?: emptyList())

        return canceller.applyFull(cancelRequest.uid,
                cancelRequest.messageId,
                cancelRequest.processedMessage,
                cancelRequest.messageType,
                cancelRequest.validateBalances)
    }

    fun processPersistResults(updateSuccessful: Boolean, messageWrapper: MessageWrapper, messageId: String) {

        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updateSuccessful

        if (!updateSuccessful) {
            val message = "Unable to save result"
            writeResponse(messageWrapper, MessageStatus.RUNTIME, message)
            LOGGER.info("$message for operation $messageId")
            return
        }

        writeResponse(messageWrapper, MessageStatus.OK)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        writeResponse(messageWrapper, status, null)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)

        message?.let {
            builder.statusReason = message
        }

        messageWrapper.writeNewResponse(builder)
    }
}