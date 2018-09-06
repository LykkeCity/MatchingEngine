package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.getSetting
import junit.framework.Assert.assertEquals
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

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashTransferOperationInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashTransferOperationInputValidatorTest {

    companion object {
        val CLIENT_NAME1 = "Client1"
        val CLIENT_NAME2 = "Client2"
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
    private lateinit var cashTransferParser: CashTransferContextParser

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var cashTransferOperationInputValidator: CashTransferOperationInputValidator

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME1, ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testAssetExists() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.assetId = "UNKNOWN"

        try {
            //when
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.UNKNOWN_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS.settingGroupName, getSetting(ASSET_ID))
        applicationSettingsCache.update()
        cashTransferOperationBuilder.volume = -1.0

        //when
        try {
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidFee() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()

        //when
        try {
            val invalidFee = ProtocolMessages.Fee.newBuilder()
                    .setType(FeeType.EXTERNAL_FEE.externalId).build()


            cashTransferOperationBuilder.addFees(invalidFee)
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_FEE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracy() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.volume = 10.001

        //when
        try {
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //when
        cashTransferOperationInputValidator.performValidation(getParsedData(getCashTransferOperationBuilder().build()))
    }

    fun getCashTransferOperationBuilder(): ProtocolMessages.CashTransferOperation.Builder {
        return ProtocolMessages.CashTransferOperation
                .newBuilder()
                .setId("test")
                .setAssetId(ASSET_ID)
                .setTimestamp(System.currentTimeMillis())
                .setFromClientId(CLIENT_NAME1)
                .setToClientId(CLIENT_NAME2).setVolume(0.0)
    }

    private fun getMessageWrapper(message: ProtocolMessages.CashTransferOperation): MessageWrapper {
        return MessageWrapper("test", MessageType.CASH_TRANSFER_OPERATION.type, message.toByteArray(), null)
    }

    private fun getParsedData(message: ProtocolMessages.CashTransferOperation): CashTransferParsedData{
        return cashTransferParser.parse(getMessageWrapper(message))
    }
}
