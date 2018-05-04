package com.lykke.matching.engine.performance

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.services.LimitOrderServiceTest
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (SingleLimitOrderPerformanceTest.Config::class)])
class SingleLimitOrderPerformanceTest : AbstractTest() {

    companion object {
        val REPEAT_TIMES = 100
    }

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 6))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testConfig(): TestConfigDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
            testSettingsDatabaseAccessor.addTrustedClient("Client3")
            return testSettingsDatabaseAccessor
        }
    }

    @Before
    fun setUp() {

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("ETHBTC", "ETH", "BTC", 5))

        initServices()
    }

    @Test
    fun testPerformance() {
        repeat(REPEAT_TIMES) {
            notEnoughFundsCase()
        }

        repeat(REPEAT_TIMES) {
            leadToNegativeSpread()
        }

        repeat(REPEAT_TIMES) {
            cancelPrevAndAddLimitOrder()
        }
    }


    private fun notEnoughFundsCase() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 500.0)

        singleLimitOrderService.processMessage(MessageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.2, volume = -501.0)))

    }

    private fun leadToNegativeSpread() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR",  500.0)
        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(price = 1.25, volume = 10.0))
        initServices()

        singleLimitOrderService.processMessage(MessageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 1.2, volume = -500.0)))
    }

    private fun cancelPrevAndAddLimitOrder() {
        singleLimitOrderService.processMessage(MessageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 200.0, volume = 1.0, uid = "2")))
        singleLimitOrderService.processMessage(MessageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(price = 500.0, volume = 1.5, uid = "3"), true))
    }
}