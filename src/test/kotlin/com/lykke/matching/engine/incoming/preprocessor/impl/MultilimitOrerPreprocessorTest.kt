package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.socket.TestClientHandler
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultilimitOrerPreprocessorTest: AbstractTest() {

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Test
    fun testLimitMultiOrderWithUnknownAssetPair() {
        //preprocessing is performed during message building
        val messageWrapper = messageBuilder.buildMultiLimitOrderWrapper(pair = "UnknownAsset",
                clientId = "Client1",
                orders = listOf(IncomingLimitOrder(100.0, 1.2),
                IncomingLimitOrder(100.0, 1.3)),
                cancel = false)

        assertResponse(messageWrapper)
    }

    @Test
    fun testStopMultiOrderWithUnknownAssetPair() {
        //preprocessing is performed during message building
        val messageWrapper = messageBuilder.buildMultiLimitOrderWrapper(pair = "UnknownAsset",

                clientId = "Client1",
                orders = listOf(IncomingLimitOrder(100.0, 1.2, type = LimitOrderType.STOP_LIMIT),
                        IncomingLimitOrder(100.0, 1.3, type = LimitOrderType.STOP_LIMIT)),
                cancel = false)

        assertResponse(messageWrapper)
    }

    private fun assertResponse(messageWrapper: MessageWrapper) {
        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)

        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.MultiLimitOrderResponse)
        response as ProtocolMessages.MultiLimitOrderResponse
        assertEquals(MessageStatus.UNKNOWN_ASSET.type, response.status)

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
    }
}