package com.lykke.matching.engine.messages

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.socket.ClientHandler
import org.apache.log4j.Logger
import java.io.IOException

class MessageWrapper(val type: Byte, val byteArray: ByteArray, val clientHandler: ClientHandler?) {

    companion object {
        val LOGGER = Logger.getLogger(MessageWrapper::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun writeResponse(response: ProtocolMessages.Response) {
        if (clientHandler != null) {
            try {
                clientHandler.writeOutput(toByteArray(MessageType.RESPONSE.type, response.serializedSize, response.toByteArray()))
            } catch (exception: IOException){
                LOGGER.error("Unable to write response: ${exception.message}", exception)
                METRICS_LOGGER.logError(this.javaClass.name, "Unable to write response: ${exception.message}", exception)
            }
        }
    }

    fun toByteArray(type: Byte, size: Int, data: ByteArray): ByteArray {
        val result = ByteArray(5 + data.size)
        result[0] = type
        //convert to little endian
        result[1] = size.toByte()
        result[2] = size.ushr(8).toByte()
        result[3] = size.ushr(16).toByte()
        result[4] = size.ushr(24).toByte()

        System.arraycopy(data, 0, result, 5, data.size)

        return result
    }
}
