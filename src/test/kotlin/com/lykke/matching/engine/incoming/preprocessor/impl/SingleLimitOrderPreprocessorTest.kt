package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.messages.MessageStatus
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
class SingleLimitOrderPreprocessorTest: AbstractTest() {

    @Autowired
    private lateinit var messageBuilder: MessageBuilder
    @Autowired
    private lateinit var singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor

    @Test
    fun testOrderWithUnknownAssetPair() {
        val messageWrapper = messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "UnknownAssetPair"))
        singleLimitOrderPreprocessor.preProcess(messageWrapper)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)

        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.NewResponse)
        response as ProtocolMessages.NewResponse
        assertEquals(MessageStatus.UNKNOWN_ASSET.type, response.status)

        assertEquals(0, clientsEventsQueue.size)
    }

    @Test
    fun testStopOrderWithUnknownAssetPair() {
        val messageWrapper = messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "UnknownAssetPair",
                type = LimitOrderType.STOP_LIMIT, lowerPrice = 1.0, lowerLimitPrice = 1.0))
        singleLimitOrderPreprocessor.preProcess(messageWrapper)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)

        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.NewResponse)
        response as ProtocolMessages.NewResponse
        assertEquals(MessageStatus.UNKNOWN_ASSET.type, response.status)

        assertEquals(0, clientsEventsQueue.size)
    }
}