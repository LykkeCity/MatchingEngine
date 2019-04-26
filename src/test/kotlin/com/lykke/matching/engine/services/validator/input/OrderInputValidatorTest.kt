package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.OrderInputValidator
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.getSetting
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
@SpringBootTest(classes = [(TestApplicationContext::class), (OrderInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderInputValidatorTest: AbstractTest() {
    private companion object {
        val MIN_VOLUME_ASSET_PAIR = AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2))
        val BTC_USD_ASSET_PAIR = AssetPair("BTCUSD", "BTC", "USD", 8)
    }

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testSettingsDatabaseAccessor(): TestSettingsDatabaseAccessor {
            val testConfigDatabaseAccessor = TestSettingsDatabaseAccessor()
            testConfigDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS, getSetting("JPY"))
            return testConfigDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var orderInputValidator: OrderInputValidator

    @Autowired
    private lateinit var assetPairHolder: AssetsPairsHolder

    @Before
    fun init() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        initServices()
    }

    @Test
    fun testCheckVolume() {
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 1.0), BTC_USD_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.1), BTC_USD_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.00000001), BTC_USD_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 1.0), BTC_USD_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.1), BTC_USD_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.00000001), BTC_USD_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 1.0, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.1, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.09, straight = false), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -1.0, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.1, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { orderInputValidator.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.09, straight = false), MIN_VOLUME_ASSET_PAIR) }
    }

    @Test(expected = Exception::class)
    fun unknownAsset() {
        try {
            orderInputValidator.validateAsset(null, "Unknown")
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.UnknownAsset, e.orderStatus)
        }


        try {
            orderInputValidator.validateAsset(assetPairHolder.getAssetPair("JPYUSD"), "Unknown")
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.DisabledAsset, e.orderStatus)
        }
    }
}