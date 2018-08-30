package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.setting.AvailableSettingGroups
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
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
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashOperationValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashOperationValidatorTest {

    companion object {
        val CLIENT_NAME = "Client1"
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
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testBalanceValidation() {
        //given
        val cashOperationBuilder = getDefaultCashOperationBuilder()
        cashOperationBuilder.amount = -60.0
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 100.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 50.0)

        //when
        try {
            cashOperationValidator.performValidation(cashOperationBuilder.build())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testAccuracy() {
        //given
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        val cashOperationBuilder = getDefaultCashOperationBuilder()

        cashOperationBuilder.amount = -60.081

        //when
        try {
            cashOperationValidator.performValidation(cashOperationBuilder.build())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testAssetDisabled() {
        //given
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroups.DISABLED_ASSETS.name, getSetting(ASSET_ID))
        applicationSettingsCache.update()

        val cashOperationBuilder = getDefaultCashOperationBuilder()
        cashOperationBuilder.amount = -1.0

        //when
        try {
            cashOperationValidator.performValidation(cashOperationBuilder.build())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }


    @Test
    fun testValidData() {
        //when
        cashOperationValidator.performValidation(getDefaultCashOperationBuilder().build())
    }

    fun getDefaultCashOperationBuilder(): ProtocolMessages.CashOperation.Builder {
        return ProtocolMessages.CashOperation.newBuilder()
                .setAssetId(ASSET_ID)
                .setClientId(CLIENT_NAME)
                .setAmount(0.0)
                .setTimestamp(System.currentTimeMillis())
                .setBussinesId("test")
                .setSendToBitcoin(false)
                .setUid(1)
    }
}