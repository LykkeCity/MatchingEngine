package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
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

    private lateinit var currentTransactionOrderBooksHolder: CurrentTransactionOrderBooksHolder

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Before
    fun setUp() {
        val genericLimitOrderService = Mockito.mock(GenericLimitOrderService::class.java)

        Mockito.`when`(genericLimitOrderService.getOrderBook("EURUSD"))
                .thenReturn(AssetOrderBook("EURUSD"))

        currentTransactionOrderBooksHolder = CurrentTransactionOrderBooksHolder(genericLimitOrderService)
    }

    @Test
    fun testGetPersistenceDataAfterCreatingAndChangingCopyOfNewOrder() {
        val order = buildLimitOrder(assetId = "EURUSD", status = "Status1", uid = "NewOrderToChange")
        currentTransactionOrderBooksHolder.addOrder(order)
        currentTransactionOrderBooksHolder.addOrder(buildLimitOrder(assetId = "EURUSD", uid = "OtherNewOrder"))
        currentTransactionOrderBooksHolder.getOrPutOrderCopyWrapper(order)
                .copy
                .updateStatus(OrderStatus.Processing, Date())

        val persistenceData = currentTransactionOrderBooksHolder.getPersistenceData()

        assertEquals(2, persistenceData.ordersToSave.size)
        assertEquals(OrderStatus.Processing.name, persistenceData.ordersToSave.single { it.externalId == "NewOrderToChange" }.status)
    }

    @Test
    fun oneSubTransactionTest() {
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.4, volume = -100.0))

        val mainOrderBookHolder = CurrentTransactionOrderBooksHolder(genericLimitOrderService)

        val workingOrderBook = PriorityBlockingQueue(mainOrderBookHolder.getOrderBook("EURUSD").getOrderBook(false))
        mainOrderBookHolder.removeOrdersFromMapsAndSetStatus(listOf(workingOrderBook.poll()))

        val uncompletedOriginalOrderBookFromMainOrderHolder = workingOrderBook.poll()
        val uncompletedFirstOrderCopy = mainOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder)
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
        val uncompletedOrderSecondCopy = secondOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderFromSecondOrderBook)
        uncompletedOrderSecondCopy.copy.remainingVolume = BigDecimal.valueOf(-80.0)
        uncompletedOrderSecondCopy.copy.updateStatus(OrderStatus.Processing, Date())

        //changes from second order book is not applied yet, so uncompleted order should contain changes only from main transaction
        assertEquals(BigDecimal.valueOf(-90.0), mainOrderBookHolder.getOrPutOrderCopyWrapper(uncompletedOriginalOrderBookFromMainOrderHolder).copy.remainingVolume)

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