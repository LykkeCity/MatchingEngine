package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertNotNull
import com.lykke.matching.engine.utils.assertEquals
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (RoundingTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RoundingTest : AbstractTest() {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()


            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0))
            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var assetsCache: AssetsCache

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 2))
        initServices()
    }

    @Test
    fun testStraightBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.11548), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1.12", marketOrderReport.trades.first().marketVolume)
        assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.12), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1498.88), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testStraightSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.11548), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1.11", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(998.89), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1.11), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.11548), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.90", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.9), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(1499.1), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
    }

    @Test
    fun testNotStraightSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.11548), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("0.89", marketOrderReport.trades.first().limitVolume)
        assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.11), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.89), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 909.727, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "CHF", 1.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = -0.38, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(909.727), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.38", marketOrderReport.trades.first().marketVolume)
        assertEquals("CHF", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("0.00041770", marketOrderReport.trades.first().limitVolume)
        assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(0.9995823), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.38), testWalletDatabaseAccessor.getBalance("Client3", "CHF"))
        assertEquals(BigDecimal.valueOf(0.0004177), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(0.62), testWalletDatabaseAccessor.getBalance("Client4", "CHF"))
    }

    @Test
    fun testStraightBuyBTC() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(678.229), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("678.23", marketOrderReport.trades.first().marketVolume)
        assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1.00000000", marketOrderReport.trades.first().limitVolume)
        assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(678.23), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(821.77), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testStraightSellBTC() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(678.229), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1.00000000", marketOrderReport.trades.first().marketVolume)
        assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("678.22", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(321.78), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(678.22), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightBuyBTC() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(678.229), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.00147443", marketOrderReport.trades.first().marketVolume)
        assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00147443), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(1499.99852557), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
    }

    @Test
    fun testNotStraightSellBTC() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(678.229), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("0.00147442", marketOrderReport.trades.first().limitVolume)
        assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(999.99852558), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00147442), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightSellBTCMultiLevel() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14925.09, volume = -1.34, clientId = "Client3"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14950.18, volume = -1.34, clientId = "Client3"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14975.27, volume = -1.34, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "LKK", 50800.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = -50800.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(14945.93), marketOrderReport.order.price!!)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(50800.0), testWalletDatabaseAccessor.getBalance("Client3", "LKK"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testNotStraightBuyEURJPY() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 116.356, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "JPY", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 0.00999999999999999)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 1.16, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.NotEnoughFunds.name, marketOrderReport.order.status)
    }

    @Test
    fun testStraightSellBTCEUR() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 597.169, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.0001)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(597.169), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.00010000", marketOrderReport.trades.first().marketVolume)
        assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("0.05", marketOrderReport.trades.first().limitVolume)
        assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(0.0001), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.95), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(0.9999), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(0.05), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
    }

    @Test
    fun testLimitOrderRounding() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 1121.509, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.00043722)))

        val limitOrder = testOrderDatabaseAccessor.getOrders("BTCEUR", true).singleOrNull()
        assertNotNull(limitOrder)
        assertEquals(BigDecimal.valueOf(1000.0 - 0.00043722), limitOrder!!.remainingVolume)
    }
}