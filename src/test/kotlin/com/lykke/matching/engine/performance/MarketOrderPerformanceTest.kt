package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.services.MarketOrderServiceTest
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Assert
import org.junit.Test

class MarketOrderPerformanceTest: AbstractPerformanceTest() {

    companion object {
        val REPEAT_TIMES = 100
    }

    override fun initServices() {
        super.initServices()

        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("SLR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("GBP", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC1", 8))

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
    }

    @Test
    fun testPerformance() {
        val averageOrderProcessionTime = Runner.runTests(MultiLimitOrderServicePerformanceTest.REPEAT_TIMES, ::testNoLiqudity,
                ::testNotEnoughFundsClientOrder, ::testNotEnoughFundsClientMultiOrder, ::testNoLiqudityToFullyFill,
                ::testNotEnoughFundsMarketOrder, ::testSmallVolume, ::testMatchOneToOne, ::testMatchOneToOneAfterNotEnoughFunds,
                ::testMatchOneToMany, ::testMatchOneToOneEURJPY, ::testMatchOneToMany2016Dec12, ::testNotStraight,
                ::testNotStraightMatchOneToMany)

        println("Multilimit order average processing time is: $averageOrderProcessionTime nanoseconds")
    }

    fun testNoLiqudity(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder())) }

        return counter.getAverageTime()
    }

    fun testNotEnoughFundsClientOrder(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        initServices()

        counter.executeAction {marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1000.0)))}
        return counter.getAverageTime()
    }

    fun testNotEnoughFundsClientMultiOrder(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))

        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1500.0))) }

        return counter.getAverageTime()
    }

    fun testNoLiqudityToFullyFill(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -2000.0))) }
        return counter.getAverageTime()
    }

    fun testNotEnoughFundsMarketOrder(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0))) }
        return counter.getAverageTime()
    }

    fun testSmallVolume(): Double {
        val counter = ActionTimeCounter()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 0.1, 0.2))
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = 0.09))) }
        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = -0.19, straight = false))) }
        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = 0.2, straight = false))) }

        return counter.getAverageTime()
    }

    fun testMatchOneToOne(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "USD",1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()

        counter.executeAction {marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))}
        return counter.getAverageTime()
    }

    fun testMatchOneToOneEURJPY(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURJPY", price = 122.512, volume = 1000000.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURJPY", price = 122.524, volume = -1000000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "JPY", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 5000000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 0.1)
        testBalanceHolderWrapper.updateBalance("Client4", "JPY", 100.0)
        initServices()

        counter.executeAction {marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 10.0, straight = false)))}

        return counter.getAverageTime()
    }

    fun testMatchOneToOneAfterNotEnoughFunds(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0))) }
        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0))) }
        return counter.getAverageTime()
    }

    fun testMatchOneToMany(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 100.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.4, volume = 1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1560.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",1400.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 150.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1000.0)
        initServices()

        counter.executeAction {marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))}
        return counter.getAverageTime()
    }

    fun testMatchOneToMany2016Dec12(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008826, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008844, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008861, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008879, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008897, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008914, volume = -4000.0, clientId = "Client1"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "SLRBTC", price = 0.00008932, volume = -4000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "SLR", 100000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 31.95294)
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "SLRBTC", volume = 25000.0, straight = true))) }

        return counter.getAverageTime()
    }

    fun testNotStraight(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = -500.0, assetId = "EURUSD", clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 750.0)
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -750.0, straight = false))) }
        return counter.getAverageTime()
    }

    fun testNotStraightMatchOneToMany(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.4, volume = -100.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = -1000.0, clientId = "Client1"))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 2000.0)
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1490.0, straight = false))) }
        return counter.getAverageTime()
    }
}