package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.MessageBuilder
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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderInputValidatorTest {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun dictionariesDatabaseAccessor(): DictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5,
                    BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    @Test
    fun testCheckVolume() {
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.1))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.00000001))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = 1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = 0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = 0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = -1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = -0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(volume = -0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = 1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = -1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.1))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.00000001))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -1.0))}
        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -0.1))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -0.09))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 1.0, straight = false))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 0.1, straight = false))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = 0.09, straight = false))}

        assertTrue { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -1.0, straight = false))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -0.1, straight = false))}
        assertFalse { limitOrderInputValidator.checkVolume( MessageBuilder.buildMarketOrder(volume = -0.09, straight = false))}
    }
}