package com.lykke.matching.engine.messages

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
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
}
