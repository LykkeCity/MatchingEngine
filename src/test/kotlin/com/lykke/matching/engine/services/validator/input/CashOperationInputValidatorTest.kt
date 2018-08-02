package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashOperationContextParser
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validator.CashOperationValidatorTest
import com.lykke.matching.engine.services.validators.CashOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashOperationInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashOperationInputValidatorTest {
    companion object {
        val ASSET_ID = "USD"
    }

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset(ASSET_ID, 2))
            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var cashOperationValidator: CashOperationValidator

    @Autowired
    private lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var cashOperationContextParser: CashOperationContextParser

    @Test(expected = ValidationException::class)
    fun testAccuracy() {
        //given
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        val cashOperationBuilder = getDefaultCashOperationBuilder()

        cashOperationBuilder.amount = -60.081

        //when
        try {
            cashOperationValidator.performValidation(getParsedData(cashOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testAssetDisabled() {
        //given
        testConfigDatabaseAccessor.addDisabledAsset(CashOperationValidatorTest.ASSET_ID)
        applicationSettingsCache.update()

        val cashOperationBuilder = getDefaultCashOperationBuilder()
        cashOperationBuilder.amount = -1.0

        //when
        try {
            cashOperationValidator.performValidation(getParsedData(cashOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }

    fun getParsedData(cashOperationMessages: ProtocolMessages.CashOperation): CashOperationParsedData {
        return cashOperationContextParser
                .parse(MessageWrapper("test", 1, cashOperationMessages.toByteArray(), null))

    }

    fun getDefaultCashOperationBuilder(): ProtocolMessages.CashOperation.Builder {
        return ProtocolMessages.CashOperation.newBuilder()
                .setAssetId(CashOperationValidatorTest.ASSET_ID)
                .setClientId(CashOperationValidatorTest.CLIENT_NAME)
                .setAmount(0.0)
                .setTimestamp(System.currentTimeMillis())
                .setBussinesId("test")
                .setSendToBitcoin(false)
                .setUid(1)
    }
}