package com.lykke.matching.engine.messages

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
import java.io.IOException

class MessageWrapper(val sourceIp: String, val type: Byte, val byteArray: ByteArray, val clientHandler: ClientHandler?) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageWrapper::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun writeResponse(response: ProtocolMessages.Response) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(MessageType.RESPONSE.type, response.serializedSize, response.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write response: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[$sourceIp]: Unable to write response", exception)
            }
        }
    }

    fun writeNewResponse(response: ProtocolMessages.NewResponse) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(MessageType.NEW_RESPONSE.type, response.serializedSize, response.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write response: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[$sourceIp]: Unable to write response", exception)
            }
        }
    }

    fun writeMarketOrderResponse(response: ProtocolMessages.MarketOrderResponse) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(MessageType.MARKER_ORDER_RESPONSE.type, response.serializedSize, response.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write response: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[$sourceIp]: Unable to write response", exception)
            }
        }
    }

    fun writeMultiLimitOrderResponse(response: ProtocolMessages.MultiLimitOrderResponse) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(MessageType.MULTI_LIMIT_ORDER_RESPONSE.type, response.serializedSize, response.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("[$sourceIp]: Unable to write response: ${exception.message}", exception)
                METRICS_LOGGER.logError("[$sourceIp]: Unable to write response", exception)
            }
        }
    }
}
