package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
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
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.assertEquals
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
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 4))
            testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC1", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 8))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

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
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("ETHUSD", "ETH", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        initServices()
    }

    @Test
    fun test20062018Accuracy() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "ETH", 1.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 401.9451)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 523.99, volume = -0.63, clientId = "Client1", assetId = "ETHUSD"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 526.531, volume = -0.5, clientId = "Client2", assetId = "ETHUSD"))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "ETHUSD", straight = false, volume = -401.9451)))
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades

        assertEquals(Matched.name, marketOrderReport.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val order = event.orders.single { it.walletId == "Client3" }
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(OrderType.MARKET, order.orderType)
    }

    @Test
    fun testNoLiqudity() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder()))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.NO_LIQUIDITY, marketOrder.rejectReason)

        assertEquals(0, tradesInfoListener.getCount())
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1000.0)))
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Cancelled.name, result.orders.find { NumberUtils.equalsIgnoreScale(it.order.price, BigDecimal.valueOf(1.6)) }?.order?.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.price == "1.6" }.status)
    }

    @Test
    fun testNotEnoughFundsClientMultiOrder() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1500.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.NO_LIQUIDITY, marketOrder.rejectReason)
    }

    @Test
    fun testNoLiqudityToFullyFill() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -2000.0)))
        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.NO_LIQUIDITY, marketOrder.rejectReason)
    }

    @Test
    fun testNotEnoughFundsMarketOrder() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 900.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))
        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, marketOrder.rejectReason)
    }

    @Test
    fun testSmallVolume() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 0.09)))
        assertEquals(1, rabbitSwapListener.getCount())
        var marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(TooSmallVolume.name, marketOrderReport.order.status)

        var event = clientsEventsQueue.poll() as ExecutionEvent
        var marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.TOO_SMALL_VOLUME, marketOrder.rejectReason)

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = -0.19, straight = false)))
        assertEquals(1, rabbitSwapListener.getCount())
        marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(TooSmallVolume.name, marketOrderReport.order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.TOO_SMALL_VOLUME, marketOrder.rejectReason)

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 0.2, straight = false)))
        assertEquals(1, rabbitSwapListener.getCount())
        marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertTrue(TooSmallVolume.name != marketOrderReport.order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertTrue(OrderRejectReason.TOO_SMALL_VOLUME != marketOrder.rejectReason)
    }

    @Test
    fun testMatchOneToOne() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()

        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1000.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1500.0000", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("1.5", eventMarketOrder.price)
        assertTrue(eventMarketOrder.straight!!)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("1000.00", eventMarketOrder.trades!!.first().volume)
        assertEquals("EUR", eventMarketOrder.trades!!.first().assetId)
        assertEquals("1500.0000", eventMarketOrder.trades!!.first().oppositeVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().oppositeAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))

        assertEquals(1, tradesInfoListener.getCount())
        val tradeInfo = tradesInfoListener.getProcessingQueue().poll()
        assertEquals(BigDecimal.ZERO, tradeInfo.price)
    }

    @Test
    fun testMatchOneToOneEURJPY() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.512, volume = 1000000.0, clientId = "Client3"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.524, volume = -1000000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "JPY", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 0.1)
        testBalanceHolderWrapper.updateBalance("Client4", "JPY", 100.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 10.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(122.512), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.09", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("10.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("JPY", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("122.512", eventMarketOrder.price)
        assertFalse(eventMarketOrder.straight!!)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("0.09", eventMarketOrder.trades!!.first().volume)
        assertEquals("EUR", eventMarketOrder.trades!!.first().assetId)
        assertEquals("10.00", eventMarketOrder.trades!!.first().oppositeVolume)
        assertEquals("JPY", eventMarketOrder.trades!!.first().oppositeAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(5000000.09), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(4999990.0), testWalletDatabaseAccessor.getBalance("Client3", "JPY"))
        assertEquals(BigDecimal.valueOf(0.01), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(110.0), testWalletDatabaseAccessor.getBalance("Client4", "JPY"))
    }

    @Test
    fun testMatchOneToOneAfterNotEnoughFunds() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        var marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)
        assertEquals(0, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        var eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, eventMarketOrder.status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, eventMarketOrder.rejectReason)
        assertEquals(0, eventMarketOrder.trades?.size)

        balancesHolder.updateBalance(
                ProcessedMessage(MessageType.BALANCE_UPDATE.type, System.currentTimeMillis(), "test"), 0, "Client4", "EUR", BigDecimal.valueOf(1000.0))
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("1.5", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1500.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testMatchOneToMany() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 100.0, clientId = "Client3"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.4, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1560.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  1400.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 150.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.41), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("1.41", eventMarketOrder.price)
        assertEquals(2, eventMarketOrder.trades?.size)

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

        assertEquals(1, tradesInfoListener.getCount())
        val tradeInfo = tradesInfoListener.getProcessingQueue().poll()
        assertEquals(BigDecimal.valueOf(1.4), tradeInfo.price)
    }

    @Test
    fun testMatchOneToMany2016Nov10() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04412, volume = -20000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04421, volume = -20000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04431, volume = -20000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 6569074.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 7500.02)
        initServices()
        
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKEUR", volume = 50000.0)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(0.0442), marketOrderReport.order.price!!)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("0.0442", eventMarketOrder.price)
        assertEquals(3, eventMarketOrder.trades?.size)

        assertEquals(BigDecimal.valueOf(2209.7), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(6519074.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.valueOf(5290.32), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Nov10_2() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13611.625476, volume = 1.463935, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13586.531910, volume = 1.463935, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13561.438344, volume = 1.463935, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 12.67565686)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = 50000.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(13591.031869), marketOrderReport.order.price!!)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("13591.031869", eventMarketOrder.price)
        assertEquals(3, eventMarketOrder.trades?.size)

        assertEquals(BigDecimal.valueOf(3.67889654), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.valueOf(8.99676032), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(50000.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Nov10_3() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.0385, volume = -20000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.03859, volume = -20000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "LKK", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "GBP", 982.78)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKGBP", volume = -982.78, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(0.03851), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("0.03851", eventMarketOrder.price)
        assertEquals(2, eventMarketOrder.trades?.size)

        assertEquals(BigDecimal.valueOf(982.78), testWalletDatabaseAccessor.getBalance("Client1", "GBP"))
        assertEquals(BigDecimal.valueOf(74487.0), testWalletDatabaseAccessor.getBalance("Client1", "LKK"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "GBP"))
        assertEquals(BigDecimal.valueOf(25513.0), testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testMatchOneToMany2016Dec12() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008826, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008844, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008861, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008879, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008897, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008914, volume = -4000.0, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008932, volume = -4000.0, clientId = "Client1"))
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
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 791.37, volume = 4000.0, clientId = "Client1"))
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
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = -500.0, assetId = "EURUSD", clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 750.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -750.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.5), marketOrderReport.order.price!!)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("1.5", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(750.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(500.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightMatchOneToMany() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.4, volume = -100.0, clientId = "Client3"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = -1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 2000.0)
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1490.0, straight = false)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(1.49), marketOrderReport.order.price!!)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("1.49", eventMarketOrder.price)
        assertEquals(2, eventMarketOrder.trades?.size)

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

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4071.121, volume = -0.00662454, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4077.641, volume = -0.01166889, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4084.382, volume = -0.01980138, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4091.837, volume = -0.02316231, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4098.155, volume = -0.03013115, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4105.411, volume = -0.03790487, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4114.279, volume = -0.03841106, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4120.003, volume = -0.04839733, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4127.137, volume = -0.04879837, clientId = "Client1"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4136.9, volume = -0.06450525, clientId = "Client1"))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "BTCCHF", volume = 0.3)))

        assertEquals(1, rabbitSwapListener.getCount())
        val marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(BigDecimal.valueOf(4111.117), marketOrderReport.order.price!!)
        assertEquals(10, marketOrderReport.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("4111.117", eventMarketOrder.price)
        assertEquals(10, eventMarketOrder.trades?.size)

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
        testOrderBookWrapper.addLimitOrder(order)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        val cancelledOrder = result.orders.filter { it.order.status == OrderStatus.Cancelled.name }
        assertEquals(1, cancelledOrder.size)
        assertEquals("Client1", cancelledOrder.first().order.clientId)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate

        val filteredBalances = balanceUpdate.balances.filter { it.id == "Client1" }
        assertEquals(1, filteredBalances.size)
        val refund = filteredBalances.first()
        assertEquals(BigDecimal.ZERO, refund.newReserved)

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventCancelledOrders = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }
        assertEquals(1, eventCancelledOrders.size)
        assertEquals("Client1", eventCancelledOrders.single().walletId)

        val eventBalanceUpdate = event.balanceUpdates!!.single { it.walletId == "Client1" }
        assertEquals("0", eventBalanceUpdate.newReserved)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0, reservedVolume = 1.19))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertBalance("Client1", "USD", 1000.0, 0.0)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder3() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 2.1))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(assetId = "EURUSD", volume = -2.0, clientId = "Client2")))

        assertEquals(1, balanceUpdateHandlerTest.balanceUpdateQueue.size)
        assertEquals(0, (balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate).balances.filter { it.id == "Client1" }.size)

        val balanceUpdates = (clientsEventsQueue.poll() as ExecutionEvent).balanceUpdates
        assertEquals(0, balanceUpdates!!.filter { it.walletId == "Client1" }.size)
    }

    @Test
    fun testMatchSellMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 50.00)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.01000199)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 49.99)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", price = 5000.0, volume = 0.01, clientId = "Client1")))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", price = 4999.0, volume = 0.01, clientId = "Client3")))

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "BTCEUR", volume = -0.01000199)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val order = event.orders.single { it.walletId == "Client2" }
        assertEquals(OutgoingOrderStatus.REJECTED, order.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME_ACCURACY, order.rejectReason)
        assertEquals(0, order.trades?.size)

        assertOrderBookSize("BTCEUR", true, 2)
    }

    @Test
    fun testStraightOrderMaxValue() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10001.0)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxValue = BigDecimal.valueOf(10000)))
        assetPairsCache.update()

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 1.0, price = 10001.0))

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = -1.0)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VALUE, eventOrder.rejectReason)

        assertOrderBookSize("BTCUSD", true, 1)
    }

    @Test
    fun testNotStraightOrderMaxValue() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxValue = BigDecimal.valueOf(10000)))
        assetPairsCache.update()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = 10001.0, straight = false)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VALUE, eventOrder.rejectReason)
    }

    @Test
    fun testStraightOrderMaxVolume() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxVolume = BigDecimal.valueOf(1.0)))
        assetPairsCache.update()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = -1.1)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME, eventOrder.rejectReason)
    }

    @Test
    fun testNotStraightOrderMaxVolume() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 11000.0)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxVolume = BigDecimal.valueOf(1.0)))
        assetPairsCache.update()

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 1.1, price = 10000.0))

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = 11000.0, straight = false)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME, eventOrder.rejectReason)

        assertOrderBookSize("BTCUSD", true, 1)
    }

    @Test
    fun testBuyPriceDeviationThreshold() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 2.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 3.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.1, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = -1.0))

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.ZERO)
        applicationSettingsCache.update()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = 2.0)))
        var eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, eventOrder.rejectReason)

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.valueOf(0.04))
        applicationSettingsCache.update()

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = 2.0)))
        eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, eventOrder.rejectReason)

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.valueOf(0.05))
        applicationSettingsCache.update()

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = 2.0)))
        eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventOrder.status)
    }

    @Test
    fun testSellPriceDeviationThreshold() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 2.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 3.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "EURUSD", price = 1.0, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "EURUSD", price = 0.9, volume = 1.0))

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.ZERO)
        applicationSettingsCache.update()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "EURUSD", volume = -2.0)))
        var eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, eventOrder.rejectReason)

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.valueOf(0.04))
        applicationSettingsCache.update()

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "EURUSD", volume = -2.0)))
        eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, eventOrder.rejectReason)

        testConfigDatabaseAccessor.addMarketOrderPriceDeviationThreshold("EURUSD", BigDecimal.valueOf(0.05))
        applicationSettingsCache.update()

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "EURUSD", volume = -2.0)))
        eventOrder = (clientsEventsQueue.single() as ExecutionEvent).orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventOrder.status)
    }
}
