package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
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
        val averageOrderProcessionTime = Runner.runTests(MultiLimitOrderServicePerformanceTest.REPEAT_TIMES, ::testNoLiqudity)
        println("Multilimit order average processing time is: $averageOrderProcessionTime ms")
    }

    fun testNoLiqudity(): Double {
        val counter = ActionTimeCounter()

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        initServices()

        counter.executeAction { marketOrderService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder())) }

        return counter.getAverageTime()
    }
}