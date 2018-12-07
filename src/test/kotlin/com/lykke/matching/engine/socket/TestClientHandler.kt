package com.lykke.matching.engine.socket

import com.google.protobuf.GeneratedMessageV3
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages

class TestClientHandler: ClientHandler {

    override var clientHostName: String? = "test localhost"

    val responses = mutableListOf<GeneratedMessageV3>()

    override fun writeOutput(byteArray: ByteArray) {
        responses.add(readResponse(byteArray))
    }

    override fun isConnected(): Boolean {
        return true
    }

    override fun disconnect() {
    }

    fun readResponse(byteArray: ByteArray): GeneratedMessageV3 {
        val type = byteArray[0]
        val serializedData = byteArray.sliceArray(IntRange(5, byteArray.size - 1))
        return when (type) {
            MessageType.NEW_RESPONSE.type -> readNewResponse(serializedData)
            MessageType.MARKER_ORDER_RESPONSE.type -> readMarketOrderResponse(serializedData)
            MessageType.MULTI_LIMIT_ORDER_RESPONSE.type -> readMultiLimitOrderResponse(serializedData)
            else -> throw Exception("Unknown response type: $type")
        }
    }

    private fun readNewResponse(serializedData: ByteArray): ProtocolMessages.NewResponse {
        return ProtocolMessages.NewResponse.parseFrom(serializedData)
    }

    private fun readMarketOrderResponse(serializedData: ByteArray): ProtocolMessages.MarketOrderResponse {
        return ProtocolMessages.MarketOrderResponse.parseFrom(serializedData)
    }

    private fun readMultiLimitOrderResponse(serializedData: ByteArray): ProtocolMessages.MultiLimitOrderResponse {
        return ProtocolMessages.MultiLimitOrderResponse.parseFrom(serializedData)
    }
}