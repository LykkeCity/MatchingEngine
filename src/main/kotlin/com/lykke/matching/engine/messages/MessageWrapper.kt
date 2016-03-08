package com.lykke.matching.engine.messages

import com.lykke.matching.engine.LOGGER
import java.io.DataOutputStream
import java.io.IOException

class MessageWrapper(val type: MessageType?, val byteArray: ByteArray, val outputStream: DataOutputStream?) {
    fun writeResponse(response: ProtocolMessages.Response) {
        if (outputStream != null) {
            try {
                outputStream.write(toByteArray(MessageType.RESPONSE.type, response.serializedSize, response.toByteArray()))
                outputStream.flush()
            } catch (exception: IOException){
                LOGGER.error("Unable to write response: ${exception.message}", exception)
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
