package com.lykke.matching.engine.utils

class ByteHelper {
    companion object {
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
}