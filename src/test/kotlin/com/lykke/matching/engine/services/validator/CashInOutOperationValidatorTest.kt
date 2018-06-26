package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
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
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashInOutOperationValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashInOutOperationValidatorTest {

    companion object {
        val CLIENT_ID = "Client1"
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
    private lateinit var cashInOutOperationValidator : CashInOutOperationValidator


    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testInvalidFee() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()

        //when
        try {
            val fee = ProtocolMessages.Fee.newBuilder()
                    .setType(FeeType.EXTERNAL_FEE.externalId).build()
            cashInOutOperationBuilder.addFees(fee)
            val feeInstructions = NewFeeInstruction.create(Arrays.asList(fee))

            cashInOutOperationValidator.performValidation(cashInOutOperationBuilder.build(), feeInstructions)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_FEE, e.validationType)
            throw e
        }

    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        testConfigDatabaseAccessor.addDisabledAsset(CashOperationValidatorTest.ASSET_ID)
        applicationSettingsCache.update()

        //when
        try {
            val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
            cashInOutOperationBuilder.volume = -1.0
            cashInOutOperationValidator.performValidation(cashInOutOperationBuilder.build(), getFeeInstructions())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testBalanceValid() {
        //given
        testBalanceHolderWrapper.updateBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 500.0)
        testBalanceHolderWrapper.updateReservedBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 250.0)
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.volume = -300.0

        //when
        try {
            cashInOutOperationValidator.performValidation(cashInOutOperationBuilder.build(), getFeeInstructions())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracy() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.volume = 10.001

        //when
        try {
            cashInOutOperationValidator.performValidation(cashInOutOperationBuilder.build(), getFeeInstructions())
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun validData() {
        cashInOutOperationValidator.performValidation(getDefaultCashInOutOperationBuilder().build(),
                getFeeInstructions())
    }

    private fun getDefaultCashInOutOperationBuilder(): ProtocolMessages.CashInOutOperation.Builder {
        return ProtocolMessages.CashInOutOperation.newBuilder()
                .setId("test")
                .setClientId(CLIENT_ID)
                .setAssetId(ASSET_ID)
                .setVolume(0.0)
                .setTimestamp(System.currentTimeMillis())
                .addFees(ProtocolMessages.Fee.newBuilder()
                        .setType(FeeType.NO_FEE.externalId))
    }

    private fun getFeeInstructions():  List<NewFeeInstruction> {
        return NewFeeInstruction.create(Arrays.asList(ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.NO_FEE.externalId)
                .build()))
    }
}