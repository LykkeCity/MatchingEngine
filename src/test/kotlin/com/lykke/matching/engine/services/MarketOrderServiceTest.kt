package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.TooSmallVolume
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.NumberUtils
import org.junit.Assert.assertEquals
import com.lykke.matching.engine.utils.assertEquals
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
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderServiceTest: AbstractTest() {
    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0))
            testBackOfficeDatabaseAccessor.addAsset(Asset("SLR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("GBP", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC1", 8))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 6))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTC1USD", "BTC1", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("SLRBTC", "SLR", "BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKKEUR", "LKK", "EUR", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKKGBP", "LKK", "GBP", 5))
        initServices()
    }

    @Test
    fun testNoLiqudity() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder()))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1000.0)))
        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Cancelled.name, result.orders.find { NumberUtils.equalsIgnoreScale(it.order.price, BigDecimal.valueOf(1.6)) }?.order?.status)
    }

    @Test
    fun testNotEnoughFundsClientMultiOrder() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1500.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNoLiqudityToFullyFill() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -2000.0)))
        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNotEnoughFundsMarketOrder() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 900.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))
        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)
    }

    @Test
    fun testSmallVolume() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 0.09)))
        assertEquals(1, rabbitSwapQueue.size)
        var marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(TooSmallVolume.name, marketOrderReport.order.status)

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = -0.19, straight = false)))
        assertEquals(1, rabbitSwapQueue.size)
        marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(TooSmallVolume.name, marketOrderReport.order.status)

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 0.2, straight = false)))
        assertEquals(1, rabbitSwapQueue.size)
        marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertTrue(TooSmallVolume.name != marketOrderReport.order.status)
    }

    @Test
    fun testMatchOneToOne() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()

        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1000.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1500.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testMatchOneToOneEURJPY() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.512, volume = 1000000.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.524, volume = -1000000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "JPY", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 0.1)
        testBalanceHolderWrapper.updateBalance("Client4", "JPY", 100.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 10.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(122.512), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.09", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("10.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("JPY", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(BigDecimal.valueOf(5000000.09), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(4999990.0), testWalletDatabaseAccessor.getBalance("Client3", "JPY"))
        assertEquals(BigDecimal.valueOf(0.01), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(110.0), testWalletDatabaseAccessor.getBalance("Client4", "JPY"))
    }

    @Test
    fun testMatchOneToOneAfterNotEnoughFunds() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        var marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)
        assertEquals(0, marketOrderReport.trades.size)

        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)

        balancesHolder.updateBalance(
                ProcessedMessage(MessageType.BALANCE_UPDATE.type, System.currentTimeMillis(),"test"),"Client4", "EUR", BigDecimal.valueOf(1000.0))
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)
        assertEquals(Matched.name, marketOrderReport.order.status)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testMatchOneToMany() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 100.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1560.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  1400.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 150.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.41), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(900.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(300.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(140.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1410.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))

        val dbBids = testOrderDatabaseAccessor.getOrders("EURUSD", true)
        assertEquals(1, dbBids.size)
        assertEquals(OrderStatus.Processing.name, dbBids.first().status)
    }

    @Test
    fun testMatchOneToMany2016Nov10() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04412, volume = -20000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04421, volume = -20000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04431, volume = -20000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 6569074.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 7500.02)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKEUR", volume = 50000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(0.0442), marketOrderReport.order.price!!)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(2209.7), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(6519074.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.valueOf(5290.32), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Nov10_2() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13611.625476, volume = 1.463935, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13586.531910, volume = 1.463935, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13561.438344, volume = 1.463935, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 12.67565686)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = 50000.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(13591.031869), marketOrderReport.order.price!!)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(3.67889654), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.valueOf(8.99676032), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Nov10_3() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.0385, volume = -20000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.03859, volume = -20000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "GBP", 982.78)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKGBP", volume = -982.78, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(0.03851), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(982.78), testWalletDatabaseAccessor.getBalance("Client1", "GBP"))
        assertEquals(BigDecimal.valueOf(74487.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "GBP"))
        assertEquals(BigDecimal.valueOf(25513.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Dec12() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008826, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008844, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008861, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008879, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008897, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008914, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008932, volume = -4000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "SLR", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 31.95294)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC", volume = 25000.0, straight = true)))

        assertEquals(BigDecimal.valueOf(2.21816), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(75000.0), testWalletDatabaseAccessor.getBalance("Client1", "SLR"))
        assertEquals(BigDecimal.valueOf(29.73478), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(25000.0), testWalletDatabaseAccessor.getBalance("Client4", "SLR"))
    }

    @Test
    fun testMatchOneToMany2016Dec12_2() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 791.37, volume = 4000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "CHF", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 0.00036983)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = -0.00036983, straight = true)))

        assertEquals(BigDecimal.valueOf(0.00036983), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(99999.71), testWalletDatabaseAccessor.getBalance("Client1", "CHF"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(0.29), testWalletDatabaseAccessor.getBalance("Client4", "CHF"))
    }

    @Test
    fun testNotStraight() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -500.0, assetId = "EURUSD", clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 750.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -750.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(750.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(500.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightMatchOneToMany() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = -100.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 2000.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1490.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.49), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(2900.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(140.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(2100.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(1350.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(510.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testMatch1() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 100028.39125545)
        testBalanceHolderWrapper.updateBalance("Client3", "CHF", 182207.39)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4071.121, volume = -0.00662454, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4077.641, volume = -0.01166889, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4084.382, volume = -0.01980138, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4091.837, volume = -0.02316231, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4098.155, volume = -0.03013115, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4105.411, volume = -0.03790487, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4114.279, volume = -0.03841106, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4120.003, volume = -0.04839733, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4127.137, volume = -0.04879837, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4136.9, volume = -0.06450525, clientId = "Client1"))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "BTCCHF", volume = 0.3)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(4111.117), marketOrderReport.order.price!!)
        assertEquals(10, marketOrderReport.trades.size)

        assertEquals(BigDecimal.valueOf(4136.9), genericLimitOrderService.getOrderBook("BTCCHF").getAskPrice())
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder1() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)

        val order = buildLimitOrder(assetId = "EURUSD", price = 1.2, volume = 1.0, clientId = "Client1")
        order.reservedLimitVolume = BigDecimal.valueOf(1.19)
        testOrderDatabaseAccessor.addLimitOrder(order)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(0, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)
        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        val cancelledOrder = result.orders.filter { it.order.status == OrderStatus.Cancelled.name }
        assertEquals(1, cancelledOrder.size)
        assertEquals("Client1", cancelledOrder.first().order.clientId)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate

        val filteredBalances = balanceUpdate.balances.filter { it.id == "Client1" }
        assertEquals(1, filteredBalances.size)
        val refund = filteredBalances.first()
        assertEquals(BigDecimal.ZERO, refund.newReserved)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0, reservedVolume = 1.19))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder3() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertEquals(1, balanceUpdateHandlerTest.balanceUpdateQueue.size)
        assertEquals(0, (balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate).balances.filter { it.id == "Client1" }.size)
    }
}
