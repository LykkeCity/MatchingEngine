package com.lykke.matching.engine.messages

import com.lykke.matching.engine.LOGGER
import java.io.IOException
import java.io.ObjectOutputStream

class MessageWrapper(val type: MessageType?, val byteArray: ByteArray, val outputStream: ObjectOutputStream?) {
    fun writeResponse(response: ProtocolMessages.Response) {
        if (outputStream != null) {
            try {
                outputStream.write(toByteArray(MessageType.RESPONSE.type, response.serializedSize, response.toByteArray()))
                outputStream.flush()
            } catch (exception: IOException){
                LOGGER.error("Unable to write response: ", exception)
            }
        }
    }

    fun toByteArray(type: Byte, size: Int, data: ByteArray): ByteArray {
        val result = ByteArray(5 + data.size)
        result[0] = type
        result[1] = size.ushr(24).toByte()
        result[2] = size.ushr(16).toByte()
        result[3] = size.ushr(8).toByte()
        result[4] = size.toByte()

        System.arraycopy(data, 0, result, 5, data.size)

        return result
    }
}
