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
        var parsedMessage: MessageOrBuilder? = null) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageWrapper::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        const val MESSAGE_ID_FIELD_NAME = "messageId"
    }

    fun writeResponse(response: ProtocolMessages.Response) {
        val resultResponse = ProtocolMessages.Response
                .newBuilder()
                .mergeFrom(response)
                .setMessageId(messageId)
                .build()

        writeClientResponse(resultResponse)
    }

    fun writeNewResponse(response: ProtocolMessages.NewResponse) {
        val resultResponse = ProtocolMessages.NewResponse
                .newBuilder()
                .mergeFrom(response)
                .setMessageId(messageId)
                .build()

        writeClientResponse(resultResponse)
    }

    fun writeMarketOrderResponse(response: ProtocolMessages.MarketOrderResponse) {
        val resultResponse = ProtocolMessages.MarketOrderResponse
                .newBuilder()
                .mergeFrom(response)
                .setMessageId(messageId)
                .build()

        writeClientResponse(resultResponse)
    }

    fun writeMultiLimitOrderResponse(response: ProtocolMessages.MultiLimitOrderResponse) {
        val resultResponse = ProtocolMessages.MultiLimitOrderResponse
                .newBuilder()
                .mergeFrom(response)
                .setMessageId(messageId)
                .build()

        writeClientResponse(resultResponse)
    }

    private fun writeClientResponse(message: Message) {
        if (clientHandler != null) {
            if (message.getField(message.getDescriptorForType().findFieldByName(MESSAGE_ID_FIELD_NAME)) == null) {
                LOGGER.error("Message id is not provided sourceIp: $sourceIp")
                throw IllegalArgumentException("Message id is not provided sourceIp: $sourceIp")
            }

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
