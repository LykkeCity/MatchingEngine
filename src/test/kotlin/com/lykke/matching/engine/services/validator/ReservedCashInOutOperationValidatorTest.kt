package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
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
 import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedCashInOutOperationValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedCashInOutOperationValidatorTest {

    companion object {
        val CLIENT_NAME = "Client"
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
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator

    @Before
    fun int() {
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 500.0)
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 550.0)
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracyInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
                .setReservedVolume(1.111)
                .build()

        //when
        try {
            reservedCashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testBalanceInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
                .setReservedVolume(-550.0)
                .build()


        //when
        try {
            reservedCashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testReservedBalanceInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
                .setReservedVolume(51.0)
                .build()

        //when
        try {
            reservedCashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.RESERVED_VOLUME_HIGHER_THAN_BALANCE, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
                .build()

        //when
        reservedCashInOutOperationValidator.performValidation(message)
    }


    private fun getDefaultReservedOperationMessageBuilder(): ProtocolMessages.ReservedCashInOutOperation.Builder {
        return ProtocolMessages.ReservedCashInOutOperation.newBuilder()
                .setId("test")
                .setClientId(CLIENT_NAME)
                .setTimestamp(System.currentTimeMillis())
                .setAssetId(ASSET_ID)
                .setReservedVolume(0.0)
    }
}