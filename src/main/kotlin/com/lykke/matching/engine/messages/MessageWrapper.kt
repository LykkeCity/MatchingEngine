package com.lykke.matching.engine.messages

import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessageUtils
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.io.IOException

class MessageWrapper(
        val sourceIp: String,
        val type: Byte,
        val byteArray: ByteArray,
        val clientHandler: ClientHandler?,
        val startTimestamp: Long = System.nanoTime(),
        var timestamp: Long? = null,
        var messageId: String? = null,
        var parsedMessage: MessageOrBuilder? = null,
        var id: String? = null,
        var triedToPersist: Boolean = false,
        var persisted: Boolean = false,
        var context: Any? = null) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageWrapper::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun processedMessage(): ProcessedMessage? {
        if (ProcessedMessageUtils.isDeduplicationNotNeeded(type)) {
            return null
        }
        return ProcessedMessage(type, timestamp!!, messageId!!)
    }

    fun writeResponse(responseBuilder: ProtocolMessages.Response.Builder) {
        if (!responseBuilder.hasMessageId() && messageId != null) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasUid() && id != null) {
            responseBuilder.uid = id!!.toLong()
        }

        writeClientResponse(responseBuilder.build(), MessageType.RESPONSE)
    }

    fun writeNewResponse(responseBuilder: ProtocolMessages.NewResponse.Builder) {

        if (!responseBuilder.hasMessageId() && messageId != null) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId() && id != null) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build(), MessageType.NEW_RESPONSE)
    }

    fun writeMarketOrderResponse(responseBuilder: ProtocolMessages.MarketOrderResponse.Builder) {
        if (!responseBuilder.hasMessageId() && messageId != null) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId() && id != null) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build(), MessageType.MARKER_ORDER_RESPONSE)
    }

    fun writeMultiLimitOrderResponse(responseBuilder: ProtocolMessages.MultiLimitOrderResponse.Builder) {
        if (!responseBuilder.hasMessageId() && messageId != null) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId() && id != null) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build(), MessageType.MULTI_LIMIT_ORDER_RESPONSE)
    }

    private fun writeClientResponse(message: Message, messageType: MessageType) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(messageType.type, message.serializedSize, message.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write for message with id $messageId response: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[$sourceIp]: Unable to write response", exception)
            }
        }
    }
}
