package com.lykke.matching.engine.holders

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.services.DisabledFunctionalityRulesService
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import com.lykke.matching.engine.web.dto.OperationType
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

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

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
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, OperationType.CASH_IN.name, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isCashInDisabled(assetsHolder.getAsset("BTC")))
    }

    @Test
    fun matchAssetAndAssetPairTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, OperationType.TRADE.name, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isTradeDisabled(assetsPairsHolder.getAssetPair("BTCUSD")))
    }

    @Test
    fun notMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, OperationType.CASH_TRANSFER.name, true, "test", "test"))

        //then
        assertFalse(disabledFunctionalityRulesHolder.isCashTransferDisabled(assetsHolder.getAsset("JPY")))
    }

    @Test
    fun disabledRuleDoesNotMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, null, "BTCUSD", OperationType.TRADE.name, false, "test", "test"))

        //then
        assertFalse(disabledFunctionalityRulesHolder.isTradeDisabled(assetsPairsHolder.getAssetPair("BTCUSD")))
    }

    @Test
    fun removedRuleDoesNotMatch() {
        //given
        val ruleId = disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, OperationType.TRADE.name, true, "test", "test"))

        //when
        disabledFunctionalityRulesService.delete(ruleId, DeleteSettingRequestDto("test", "test"))

        //then
        assertFalse(disabledFunctionalityRulesHolder.isTradeDisabled(assetsPairsHolder.getAssetPair("BTCUSD")))
    }

    @Test
    fun assetMatchWithAnyMessageType() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, "BTC", null, null, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isTradeDisabled(assetsPairsHolder.getAssetPair("BTCUSD")))
        assertTrue(disabledFunctionalityRulesHolder.isCashTransferDisabled(assetsHolder.getAsset("BTC")))
    }

    @Test
    fun operationMatchTest() {
        //given
        disabledFunctionalityRulesService.create(DisabledFunctionalityRuleDto(null, null, null, OperationType.CASH_IN.name, true, "test", "test"))

        //then
        assertTrue(disabledFunctionalityRulesHolder.isCashInDisabled(assetsHolder.getAsset("BTC")))
    }
}