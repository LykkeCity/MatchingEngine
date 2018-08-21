package com.lykke.matching.engine.order

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.Date
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ExpiredOrdersCancellerTest : AbstractTest() {

    @Autowired
    private lateinit var messagesRouter: MessageRouter

    @Autowired
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    private val orders = mutableListOf<LimitOrder>()

    @Before
    fun setUp() {
        val now = Date()
        orders.addAll(listOf(buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = date(now, 500), uid = "1"),
                buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = date(now, 1500), uid = "2")))
        orders.forEach { expiryOrdersQueue.addOrder(it) }
    }

    @Test
    fun testCancelExpiredOrders() {
        val service = ExpiredOrdersCanceller(expiryOrdersQueue, messagesRouter)

        service.cancelExpiredOrders()
        assertEquals(0, messagesRouter.preProcessedMessageQueue.size)

        Thread.sleep(800)
        service.cancelExpiredOrders()
        assertEquals(1, messagesRouter.preProcessedMessageQueue.size)
        var message = ProtocolMessages.LimitOrderCancel.parseFrom(messagesRouter.preProcessedMessageQueue.poll().byteArray)
        assertEquals(1, message.limitOrderIdCount)
        assertEquals("1", message.limitOrderIdList.single())

        messagesRouter.preProcessedMessageQueue.clear()
        expiryOrdersQueue.removeOrder(orders[0])

        Thread.sleep(800)
        service.cancelExpiredOrders()
        assertEquals(1, messagesRouter.preProcessedMessageQueue.size)
        message = ProtocolMessages.LimitOrderCancel.parseFrom(messagesRouter.preProcessedMessageQueue.poll().byteArray)
        assertEquals(1, message.limitOrderIdCount)
        assertEquals("2", message.limitOrderIdList.single())
    }

    private fun date(date: Date, delta: Long) = Date(date.time + delta)
}