package com.lykke.matching.engine.message

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper.Companion.toByteArray
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.verify
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner

@RunWith(MockitoJUnitRunner::class)
class MessageWrapperTest {
    @Mock
    private lateinit var  clientHandler : ClientHandler

    companion object {
        val MESSAGE_ID1 = "messageID1"
        val MESSAGE_ID2 = "messageID2"
        val UID1 = 1L
        val UID2 = 2L
        val STATUS = MessageStatus.BALANCE_LOWER_THAN_RESERVED.type
        val ASSET = "ASSET"
    }

    @Test
    fun testWriteResponse() {
        //given
        val response = ProtocolMessages.Response.newBuilder()
        val responseMessage = ProtocolMessages.Response.newBuilder()
                .setMessageId(MESSAGE_ID1)
                .setUid(UID1)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteNewResponse() {
        //given
        val response = ProtocolMessages.NewResponse.newBuilder()
                .setStatus(STATUS)
        val responseMessage = ProtocolMessages.NewResponse.newBuilder()
                .setMessageId(MESSAGE_ID1)
                .setId(UID1.toString())
                .setStatus(STATUS)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeNewResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.NEW_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteMarketOrderResponse() {
        //given
        val response = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setId(UID1.toString())
                .setStatus(STATUS)
        val responseMessage = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setId(UID1.toString())
                .setMessageId(MESSAGE_ID1)
                .setStatus(STATUS)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeMarketOrderResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.MARKER_ORDER_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteMultiLimitOrderResponse() {
        //given
        val response = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(STATUS)
                .setAssetPairId(ASSET)
        val responseMessage = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setMessageId(MESSAGE_ID1)
                .setId(UID1.toString())
                .setStatus(STATUS)
                .setAssetPairId(ASSET)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeMultiLimitOrderResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.MULTI_LIMIT_ORDER_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteResponseIsNorErasedByMessageWrapper() {
        //given
        val response = ProtocolMessages.Response.newBuilder()
                .setUid(UID2)
                .setMessageId(MESSAGE_ID2)
        val responseMessage = ProtocolMessages.Response.newBuilder()
                .setMessageId(MESSAGE_ID2)
                .setUid(UID2)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteNewResponseIsNorErasedByMessageWrapper() {
        //given
        val response = ProtocolMessages.NewResponse.newBuilder()
                .setStatus(STATUS)
                .setId(UID2.toString())
                .setMessageId(MESSAGE_ID2)
        val responseMessage = ProtocolMessages.NewResponse.newBuilder()
                .setMessageId(MESSAGE_ID2)
                .setId(UID2.toString())
                .setStatus(STATUS)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeNewResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.NEW_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteMarketOrderResponseIsNorErasedByMessageWrapper() {
        //given
        val response = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setId(UID2.toString())
                .setMessageId(MESSAGE_ID2)
                .setStatus(STATUS)
        val responseMessage = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setId(UID2.toString())
                .setMessageId(MESSAGE_ID2)
                .setStatus(STATUS)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeMarketOrderResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.MARKER_ORDER_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }

    @Test
    fun testWriteMultiLimitOrderResponseIsNorErasedByMessageWrapper() {
        //given
        val response = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(STATUS)
                .setAssetPairId(ASSET)
                .setMessageId(MESSAGE_ID2)
                .setId(UID2.toString())
        val responseMessage = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setMessageId(MESSAGE_ID2)
                .setId(UID2.toString())
                .setStatus(STATUS)
                .setAssetPairId(ASSET)
                .build()

        val messageWrapper = MessageWrapper("test",
                MessageType.RESPONSE.type,
                ByteArray(0),
                clientHandler,
                System.currentTimeMillis(), messageId = MESSAGE_ID1, id = UID1.toString())

        //when
        messageWrapper.writeMultiLimitOrderResponse(response)

        //then
        verify(clientHandler).writeOutput(eq(toByteArray(MessageType.MULTI_LIMIT_ORDER_RESPONSE.type, responseMessage.serializedSize, responseMessage.toByteArray())))
    }
}