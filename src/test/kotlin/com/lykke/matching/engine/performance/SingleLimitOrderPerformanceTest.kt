package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.getSetting
import org.junit.Ignore
import org.junit.Test
import java.math.BigDecimal

@Ignore
class SingleLimitOrderPerformanceTest: AbstractPerformanceTest()  {

    override fun initServices() {
        super.initServices()

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 6))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS.settingGroupName, getSetting("Client3"))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("ETHBTC", "ETH", "BTC", 5))
    }

    @Test
    fun testPerformance() {
        val averageOrderProcessionTime = Runner.runTests(REPEAT_TIMES, ::notEnoughFundsCase,
                ::leadToNegativeSpread, ::cancelPrevAndAddLimitOrder, ::testNegativeSpread,
                ::testSmallVolume, ::testAddAndMatchLimitOrderRounding, ::testAddAndMatchLimitOrderWithDust,
                ::testAddAndMatchLimitOrderWithSamePrice, ::testAddAndMatchLimitSellDustOrder,
                ::testAddAndMatchBuyLimitDustOrder, ::testAddAndPartiallyMatchLimitOrder, ::testAddAndMatchWithLimitOrder,
                ::testMatchWithLimitOrderForAllFunds, ::testMatchWithOwnLimitOrder, ::testOverflowedRemainingVolume)
        println("Single limit order average processing time is:  ${PrintUtils.convertToString2(averageOrderProcessionTime)}")
    }


    private fun notEnoughFundsCase(): Double {
        initServices()
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR",  500.0)

        counter.executeAction { singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.2, volume = -501.0))) }
        return counter.getAverageTime()
    }

    private fun leadToNegativeSpread(): Double {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR",  500.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.25, volume = 10.0))
        initServices()

        val counter = ActionTimeCounter()
        counter.executeAction {singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.2, volume = -500.0)))}
        return counter.getAverageTime()
    }

    private fun cancelPrevAndAddLimitOrder(): Double {
        initServices()

        val counter = ActionTimeCounter()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 200.0, volume = 1.0, uid = "2")))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 500.0, volume = 1.5, uid = "3"), true))}
        return counter.getAverageTime()
    }

    private fun testNegativeSpread(): Double {
        initServices()

        val counter = ActionTimeCounter()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 100.0, volume = 1.0)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 200.0, volume = 1.0)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 300.0, volume = -1.0)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 150.0, volume = -1.0)))}
        return counter.getAverageTime()
    }

    private fun testSmallVolume(): Double {
        val counter = ActionTimeCounter()
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        counter.executeAction {  testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))}
        initServices()

        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(volume = 0.09)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.9, volume = 0.1)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 2.0, volume = -0.1)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrderRounding(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 4199.351, volume = 0.00357198)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", price = 4199.351, volume = -0.00357198)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrderWithDust(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)

        initServices()

        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", clientId = "Client1", price = 3200.0, volume = -0.01)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", clientId = "Client3", price = 3200.0, volume = 0.009973)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrderWithSamePrice(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 122.512, volume = 1.0)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitSellDustOrder(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1000.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3583.081, volume = 0.00746488, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3581.391, volume = 0.00253512, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3579.183, volume = 0.00253512, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3578.183, volume = 0.00253512, clientId = "Client3"))

        initServices()

        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3575.782, volume = -0.01)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchBuyLimitDustOrder(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 4000.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3827.395, volume = -0.00703833, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3830.926, volume = -0.01356452, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3832.433, volume = -0.02174805, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3836.76, volume = -0.02740016, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3838.624, volume = -0.03649953, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3842.751, volume = -0.03705699, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3845.948, volume = -0.04872587, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3847.942, volume = -0.05056858, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3851.385, volume = -0.05842735, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3855.364, volume = -0.07678406, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3858.021, volume = -0.07206853, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3861.283, volume = -0.05011803, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", price = 3863.035, volume = -0.1, clientId = "Client3"))

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client1", assetId = "BTCEUR", price = 3890.0, volume = 0.5)))}
        return counter.getAverageTime()
    }

    fun testAddAndPartiallyMatchLimitOrder(): Double {
        val counter = ActionTimeCounter()

        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 122.52, volume = 11.0)))}
        return counter.getAverageTime()
    }

    fun testAddAndMatchWithLimitOrder(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 406.24)
        testBalanceHolderWrapper.updateReservedBalance("Client4", "USD", 263.33)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", price = 4421.0, volume = -0.00045239)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client4", assetId = "BTCUSD", price = 4425.0, volume = 0.032)))}
        return counter.getAverageTime()
    }


    fun testMatchWithOwnLimitOrder(): Double {
        val counter = ActionTimeCounter()
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.0, volume = -10.0))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 10.00)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10.00)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10.00)

        initServices()
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.0, volume = 10.0)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.0, volume = 10.0, clientId = "Client2")))}
        return counter.getAverageTime()
    }

    fun testMatchWithLimitOrderForAllFunds(): Double {
        val counter = ActionTimeCounter()
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 700.04)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD",  700.04)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 2.0)

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 4722.0, volume = 0.14825226))
        initServices()

        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 4721.403, volume = -0.4435)))}
        return counter.getAverageTime()
    }

    fun testOverflowedRemainingVolume(): Double {
        val counter = ActionTimeCounter()

        testBackOfficeDatabaseAccessor.addAsset(Asset("PKT", 12))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("PKTETH", "PKT", "ETH", 5))
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "PKT", 3.0)

        initServices()

        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", assetId = "PKTETH", price = 0.0001, volume = -2.689999999998)))}
        counter.executeAction {  singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client1", assetId = "PKTETH", price = 0.0001, volume = 100.0)))}
        return counter.getAverageTime()
    }
}