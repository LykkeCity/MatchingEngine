package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CurrentTransactionOrderBooksHolderTest {

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Test
    fun oneSubTransactionTest() {
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.4, volume = -100.0))

        val mainOrderBookHolder = CurrentTransactionOrderBooksHolder(genericLimitOrderService)

        val workingOrderBook = PriorityBlockingQueue(mainOrderBookHolder.getOrderBook("EURUSD").getOrderBook(false))
        mainOrderBookHolder.removeOrdersFromMapsAndSetStatus(listOf(workingOrderBook.poll()))

        val uncompletedOriginalOrderBookFromMainOrderHolder = workingOrderBook.poll()
        val uncompletedFirstOrderCopy = mainOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder) { CopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder) }
        uncompletedFirstOrderCopy.copy.remainingVolume = BigDecimal.valueOf(-90.0)
        uncompletedFirstOrderCopy.copy.updateStatus(OrderStatus.Processing, Date())
        workingOrderBook.put(uncompletedOriginalOrderBookFromMainOrderHolder)

        mainOrderBookHolder.getChangedOrderBookCopy("EURUSD").setOrderBook(false, workingOrderBook)



        val secondOrderBookHolder = CurrentTransactionOrderBooksHolder(mainOrderBookHolder)
        assertEquals(2, secondOrderBookHolder.getOrderBook("EURUSD").getSellOrderBook().size)
        val secondWorkingOrderBook = PriorityBlockingQueue(secondOrderBookHolder.getOrderBook("EURUSD").getOrderBook(false))

        val uncompletedOriginalOrderFromSecondOrderBook = secondWorkingOrderBook.poll()

        //in order books should be only original orders
        assertTrue(uncompletedOriginalOrderBookFromMainOrderHolder === uncompletedOriginalOrderFromSecondOrderBook)
        val uncompletedOrderSecondCopy = secondOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderFromSecondOrderBook) { CopyWrapper(uncompletedOriginalOrderFromSecondOrderBook) }
        uncompletedOrderSecondCopy.copy.remainingVolume = BigDecimal.valueOf(-80.0)
        uncompletedOrderSecondCopy.copy.updateStatus(OrderStatus.Processing, Date())

        //changes from second order book is not applied yet, so uncompleted order should contain changes only from main transaction
        assertEquals(BigDecimal.valueOf(-90.0), mainOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder)  { CopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder) }.copy.remainingVolume)

        secondOrderBookHolder.apply(Date())
        assertEquals(3, genericLimitOrderService.getOrderBook("EURUSD").getSellOrderBook().size)

        mainOrderBookHolder.apply(Date())

        val resultOrderBook = genericLimitOrderService.getOrderBook("EURUSD").getSellOrderBook()
        assertEquals(2, resultOrderBook.size)
        val resultUncompletedOrder = resultOrderBook.first()

        assertEquals(BigDecimal.valueOf(-80.0),  resultUncompletedOrder.remainingVolume)
        assertEquals(OrderStatus.Processing.name,  resultUncompletedOrder.status)
    }
}