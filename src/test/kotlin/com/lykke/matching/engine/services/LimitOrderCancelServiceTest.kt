package com.lykke.matching.engine.services

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderCancelServiceTest {
    val testFileDatabaseAccessor = TestFileOrderDatabaseAccessor()
    val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()

    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor))

    @Autowired
    lateinit var balancesHolder: BalancesHolder

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

    @Autowired
    private lateinit var config: com.lykke.matching.engine.utils.config.Config

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testWalledDatabaseAccessor(): WalletDatabaseAccessor {
            val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()

            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))

            return testWalletDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "5", price = -100.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "6", price = -200.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "7", price = -300.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "8", price = -400.0))


        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
    }

    @Test
    fun testCancel() {
        val genericService = GenericLimitOrderService(testFileDatabaseAccessor, assetsHolder,
                assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)
        val service = LimitOrderCancelService(genericService, limitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, orderBookQueue, rabbitOrderBookQueue)
      
        service.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("3"))

        assertEquals(1, orderBookQueue.size)

        val order = testFileDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(4, testFileDatabaseAccessor.loadLimitOrders().size)
        val previousOrders = genericService.getAllPreviousOrders("Client1", "EURUSD", true)
        assertEquals(4, previousOrders.size)
        assertFalse(previousOrders.any { it.externalId == "3" })
    }
}