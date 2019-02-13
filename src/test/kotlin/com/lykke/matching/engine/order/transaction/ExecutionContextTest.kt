package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ExecutionContextTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ExecutionContextTest : AbstractTest() {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 4))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 4))
            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 4))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    private lateinit var executionDataApplyService: ExecutionDataApplyService

    @Test
    fun executionContextOneTransaction() {
        //given
        val executionContext = createMainExecutionContextAndPerformChanges()

        //when
        executionContext.apply()
        executionDataApplyService.persistAndSendEvents(null, executionContext)

        //then
        assertEquals(BigDecimal.valueOf(1000.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(90.0), balancesHolder.getReservedBalance("Client1", "USD"))
        val appliedOrder = genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).peek()
        assertEquals(BigDecimal.valueOf(0.9), appliedOrder.remainingVolume)
        assertEquals(OrderStatus.Processing.name, appliedOrder.status)
    }

    @Test
    fun executionContextWithSubTransaction() {
        //given
        val executionContext = createMainExecutionContextAndPerformChanges()

        //when
        val subExecutionContext = executionContextFactory.create(executionContext)

        //change trade idx in sub context
        subExecutionContext.tradeIndex = 1

        //change order in sub context
        val workingOrderBook = PriorityBlockingQueue(subExecutionContext.orderBooksHolder.getOrderBook("BTCUSD").getOrderBook(true))
        val originalOrder = workingOrderBook.poll()
        val orderCopy = subExecutionContext.orderBooksHolder.getOrPutOrderCopyWrapper(originalOrder)
        orderCopy.copy.updateRemainingVolume(BigDecimal.valueOf(0.8))

        //change balance in sub context
        subExecutionContext.walletOperationsProcessor.preProcess(listOf(WalletOperation("Client1", "USD", BigDecimal.ZERO, BigDecimal.valueOf(-10.0))))

        //change client limit order with trades in sub context
        subExecutionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(orderCopy.copy))

        //sub transaction is not yet commit so in main execution context are changes only from main transaction
        val limitOrder = executionContext.orderBooksHolder.getOrderBook("BTCUSD").getOrderBook(true).peek()
        val orderFromMainTransaction = executionContext.orderBooksHolder.getOrPutOrderCopyWrapper(limitOrder)
        assertEquals(BigDecimal.valueOf(0.9), orderFromMainTransaction.copy.remainingVolume)
        assertEquals(0, executionContext.tradeIndex)
        assertEquals(BigDecimal.valueOf(90.0), executionContext.walletOperationsProcessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(910.0), executionContext.walletOperationsProcessor.getAvailableBalance("Client1", "USD"))
        assertEquals(0, executionContext.getClientsLimitOrdersWithTrades().size)


        //when
        subExecutionContext.apply()

        //then
        //changes not yet applied - so we have original data in app cache
        assertOriginalDataInApplicationCacheNotChanged()

        //check changes from sub execution context applied to main execution context
        val updatedLimitOrder = executionContext.orderBooksHolder.getOrderBook("BTCUSD").getOrderBook(true).peek()
        val orderFromMainTransactionAfterApply = executionContext.orderBooksHolder.getOrPutOrderCopyWrapper(updatedLimitOrder)
        assertEquals(BigDecimal.valueOf(0.8), orderFromMainTransactionAfterApply.copy.remainingVolume)
        assertEquals(1, executionContext.tradeIndex)
        assertEquals(BigDecimal.valueOf(80.0), executionContext.walletOperationsProcessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(920.0), executionContext.walletOperationsProcessor.getAvailableBalance("Client1", "USD"))
        assertEquals(1, executionContext.getClientsLimitOrdersWithTrades().size)

        executionContext.apply()
        executionDataApplyService.persistAndSendEvents(null, executionContext)

        assertEquals(BigDecimal.valueOf(1000.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(80.0), balancesHolder.getReservedBalance("Client1", "USD"))
        val appliedOrder = genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).peek()
        assertEquals(BigDecimal.valueOf(0.8), appliedOrder.remainingVolume)
        assertEquals(OrderStatus.Processing.name, appliedOrder.status)
    }

    @Test
    fun executionContextWithSubTransactionPartiallyMatchedOrderInSubTransaction() {
        //given
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(uid = "5", volume = 1.0, price = 100.0, assetId = "BTCUSD"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 100.0)


        val executionContext = executionContextFactory.create("testMessageId",
                "requestId",
                MessageType.MARKET_ORDER, null,
                mapOf("BTCUSD" to assetsPairsHolder.getAssetPair("BTCUSD")),
                Date(), LoggerFactory.getLogger("test"), LoggerFactory.getLogger("test"))

        //when
        val subExecutionContext = executionContextFactory.create(executionContext)

        val workingOrderBook = PriorityBlockingQueue(subExecutionContext.orderBooksHolder.getOrderBook("BTCUSD").getOrderBook(true))
        val order = workingOrderBook.poll()

        val orderCopy = subExecutionContext.orderBooksHolder.getOrPutOrderCopyWrapper(order)
        orderCopy.copy.updateStatus(OrderStatus.Processing, Date())
        orderCopy.copy.updateRemainingVolume(BigDecimal.valueOf(0.9))
        workingOrderBook.add(orderCopy.origin)

        subExecutionContext.orderBooksHolder.getChangedOrderBookCopy("BTCUSD").setOrderBook(true, workingOrderBook)
        subExecutionContext.apply()
        executionContext.apply()

        //then
        val appliedOrder = genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).peek()
        assertEquals(BigDecimal.valueOf(0.9), appliedOrder.remainingVolume)
        assertEquals(OrderStatus.Processing.name, appliedOrder.status)
        executionDataApplyService.persistAndSendEvents(null, executionContext)
    }

    private fun createMainExecutionContextAndPerformChanges(): ExecutionContext {
        testOrderBookWrapper.addLimitOrder(MessageBuilder.buildLimitOrder(uid = "5", volume = 1.0, price = 100.0, assetId = "BTCUSD"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 100.0)

        val executionContext = executionContextFactory.create("testMessageId",
                "requestId",
                MessageType.MARKET_ORDER, null,
                mapOf("BTCUSD" to assetsPairsHolder.getAssetPair("BTCUSD")),
                Date(), LoggerFactory.getLogger("test"),
                LoggerFactory.getLogger("test"))

        val workingOrderBook = PriorityBlockingQueue(executionContext.orderBooksHolder.getOrderBook("BTCUSD").getOrderBook(true))
        val order = workingOrderBook.poll()

        val orderCopy = executionContext.orderBooksHolder.getOrPutOrderCopyWrapper(order)
        orderCopy.copy.updateStatus(OrderStatus.Processing, Date())
        orderCopy.copy.updateRemainingVolume(BigDecimal.valueOf(0.9))
        workingOrderBook.add(orderCopy.origin)

        executionContext.orderBooksHolder.getChangedOrderBookCopy("BTCUSD").setOrderBook(true, workingOrderBook)

        executionContext.walletOperationsProcessor.preProcess(listOf(WalletOperation("Client1", "USD", BigDecimal.ZERO, BigDecimal.valueOf(-10.0))))

        //changes not yet applied - so we have original data in app cache
        assertOriginalDataInApplicationCacheNotChanged()

        val newBalancesPersistenceData = executionContext.walletOperationsProcessor.persistenceData()
        assertEquals(BigDecimal.valueOf(1000.0), newBalancesPersistenceData.balances.first().balance)
        assertEquals(BigDecimal.valueOf(90.0), newBalancesPersistenceData.balances.first().reserved)

        val ordersPersistenceData = executionContext.orderBooksHolder.getPersistenceData()
        val changedOrder = ordersPersistenceData.ordersToSave.first()
        assertEquals(BigDecimal.valueOf(0.9), changedOrder.remainingVolume)
        assertEquals(OrderStatus.Processing.name, changedOrder.status)

        return executionContext
    }

    private fun assertOriginalDataInApplicationCacheNotChanged() {
        val originalOrder = genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).peek()
        assertEquals(BigDecimal.valueOf(1.0), originalOrder.remainingVolume)

        assertEquals(BigDecimal.valueOf(1000.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), balancesHolder.getReservedBalance("Client1", "USD"))
    }
}