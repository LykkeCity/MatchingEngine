package com.lykke.matching.engine.holders

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.DisabledFunctionalityRulesService
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (DisabledFunctionalityRulesHolderTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DisabledFunctionalityRulesHolderTest {

    @Autowired
    private lateinit var disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder

    @Autowired
    private lateinit var disabledFunctionalityRulesService: DisabledFunctionalityRulesService

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 4))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 4))
            testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 4))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testDictionariesDatabaseAccessor(): DictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 2))
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 2))
            return testDictionariesDatabaseAccessor
        }
    }

    @Test
    fun fullMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", "BTCUSD", 50, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule("BTC", "BTCUSD", MessageType.valueOf(50.toByte()))))
    }

    @Test
    fun notMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", "BTCUSD", 50, true, "test", "test"))

        //then
        assertFalse(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule(null, "EURJPY", MessageType.valueOf(50.toByte()))))
    }

    @Test
    fun disabledRuleDoesNotMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", "BTCUSD", 50, false, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule("BTC", "BTCUSD", MessageType.valueOf(50.toByte()))))
    }

    @Test
    fun assetMatchWithAssetPairIdTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, 50, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule(null, "BTCUSD", MessageType.valueOf(50.toByte()))))
    }

    @Test
    fun onlyAssetSetMatchWithAnyMessageType() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, null, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule(null, "BTCUSD", MessageType.LIMIT_ORDER)))
        assertTrue(disabledFunctionalityRulesHolder.isDisabled(DisabledFunctionalityRule(null, "BTCUSD", MessageType.MARKET_ORDER)))
    }
}