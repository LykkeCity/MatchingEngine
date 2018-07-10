package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashTransferOperationValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashTransferOperationValidatorTest {

    companion object {
        val CLIENT_NAME1 = "Client1"
        val CLIENT_NAME2 = "Client2"
        val ASSET_ID = "USD"
    }

    @Autowired
    private lateinit var cashTransferParser: CashTransferContextParser


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
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var cashTransferOperationValidator: CashTransferOperationValidator

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        testConfigDatabaseAccessor.addDisabledAsset(CashOperationValidatorTest.ASSET_ID)
        applicationSettingsCache.update()
        cashTransferOperationBuilder.volume = -1.0

        //when
        try {
            cashTransferOperationValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
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
            cashTransferOperationValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
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
            cashTransferOperationValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testLowBalance() {
        //given
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID, 100.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME2, ASSET_ID, 60.0)
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.overdraftLimit = -70.0

        //when
        try {
            cashTransferOperationValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //when
        cashTransferOperationValidator.performValidation(getContext(getCashTransferOperationBuilder().build()))
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

    private fun getContext(message: ProtocolMessages.CashTransferOperation): CashTransferContext {
        return cashTransferParser.parse(getMessageWrapper(message)).context as CashTransferContext
    }
}
