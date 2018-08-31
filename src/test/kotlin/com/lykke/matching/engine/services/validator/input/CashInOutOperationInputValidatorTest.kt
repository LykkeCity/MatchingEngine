package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validator.CashOperationValidatorTest
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
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
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashInOutOperationInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashInOutOperationInputValidatorTest {

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
    private lateinit var cashInOutOperationInputValidator: CashInOutOperationInputValidator

    @Autowired
    private lateinit var cashInOutContextInitializer: CashInOutContextParser

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_ID, ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_ID, ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun assetDoesNotExist() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.setAssetId("UNKNOWN")

        try {
            //when
            cashInOutOperationInputValidator
                    .performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            //then
            assertEquals(ValidationException.Validation.UNKNOWN_ASSET, e.validationType)
            throw e
        }
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

            val cashInOutContext = getParsedData(cashInOutOperationBuilder.build())
            cashInOutOperationInputValidator
                    .performValidation(cashInOutContext)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_FEE, e.validationType)
            throw e
        }

    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS.name, getSetting(ASSET_ID))
        applicationSettingsCache.update()

        //when
        try {
            val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
            cashInOutOperationBuilder.volume = -1.0
            cashInOutOperationInputValidator.performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
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
            cashInOutOperationInputValidator.performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun validData() {
        cashInOutOperationInputValidator.performValidation(getParsedData(getDefaultCashInOutOperationBuilder().build()))
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

    private fun getMessageWrapper(cashInOutOperation: ProtocolMessages.CashInOutOperation): MessageWrapper {
        return MessageWrapper("",
                MessageType.CASH_IN_OUT_OPERATION.type,
                cashInOutOperation.toByteArray(),
                clientHandler = null)
    }

    private fun getParsedData(cashInOutOperation: ProtocolMessages.CashInOutOperation): CashInOutParsedData {
        return cashInOutContextInitializer.parse(getMessageWrapper(cashInOutOperation))
    }
}