package com.lykke.matching.engine.messages

import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
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
        var id: String? = null) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageWrapper::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun writeResponse(responseBuilder: ProtocolMessages.Response.Builder) {
        if (!responseBuilder.hasMessageId()) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasUid()) {
            responseBuilder.uid = id!!.toLong()
        }

        writeClientResponse(responseBuilder.build())
    }

    fun writeNewResponse(responseBuilder: ProtocolMessages.NewResponse.Builder) {

        if (!responseBuilder.hasMessageId()) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId()) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build())
    }

    fun writeMarketOrderResponse(responseBuilder: ProtocolMessages.MarketOrderResponse.Builder) {
        if (!responseBuilder.hasMessageId()) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId()) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build())
    }

    fun writeMultiLimitOrderResponse(responseBuilder: ProtocolMessages.MultiLimitOrderResponse.Builder) {
        if (!responseBuilder.hasMessageId()) {
            responseBuilder.messageId = messageId
        }

        if (!responseBuilder.hasId()) {
            responseBuilder.id = id
        }

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(message: Message) {
        if (clientHandler != null) {
            try {
                LOGGER.info("Writing response with  messageId: $messageId")
                clientHandler.writeOutput(toByteArray(MessageType.MARKER_ORDER_RESPONSE.type, message.serializedSize, message.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write for message with id $messageId response: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[$sourceIp]: Unable to write response", exception)
            }
        }
    }
}
