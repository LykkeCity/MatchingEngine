package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashSwapOperationValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashSwapOperationValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashSwapOperationValidatorTest {
    companion object {
        val OPERATION_ID = "test"
        val ASSET_ID1 = "USD"
        val ASSET_ID2 = "EUR"
        val CLIENT_NAME1 = "Client1"
        val CLIENT_NAME2 = "Client2"
    }

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset(ASSET_ID1, 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset(ASSET_ID2, 2))
            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var cashSwapOperationValidator: CashSwapOperationValidator

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID1, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME1, ASSET_ID1, 50.0)

        testBalanceHolderWrapper.updateBalance(CLIENT_NAME2, ASSET_ID2, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME2, ASSET_ID2, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testInvalidBalanceClient1() {
        //given
        val cashSwapOperationBuilder = getCashSwapOperationBuilder()
        cashSwapOperationBuilder.volume1 = 100.0
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID1, 100.0)

        //when
        try {
            cashSwapOperationValidator.performValidation(cashSwapOperationBuilder.build(), OPERATION_ID)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidBalanceClient2() {
        //given
        val cashSwapOperationBuilder = getCashSwapOperationBuilder()
        cashSwapOperationBuilder.volume1 = 100.0
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME2, ASSET_ID2, 100.0)

        //when
        try {
            cashSwapOperationValidator.performValidation(cashSwapOperationBuilder.build(), OPERATION_ID)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidVolumeAccuracyClient1() {
        //given
        val cashSwapOperationBuilder = getCashSwapOperationBuilder()
        cashSwapOperationBuilder.volume1 = 1.001

        //when
        try {
            cashSwapOperationValidator.performValidation(cashSwapOperationBuilder.build(), OPERATION_ID)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidVolumeAccuracyClient2() {
        //given
        val cashSwapOperationBuilder = getCashSwapOperationBuilder()
        cashSwapOperationBuilder.volume2 = 1.001

        //when
        try {
            cashSwapOperationValidator.performValidation(cashSwapOperationBuilder.build(), OPERATION_ID)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }


    @Test
    fun testValidData() {
        //when
        cashSwapOperationValidator.performValidation(getCashSwapOperationBuilder().build(), OPERATION_ID)
    }

    private fun getCashSwapOperationBuilder(): ProtocolMessages.CashSwapOperation.Builder {
        return ProtocolMessages.CashSwapOperation.newBuilder()
                .setId("test")
                .setAssetId1(ASSET_ID1)
                .setAssetId2(ASSET_ID2)
                .setClientId1(CLIENT_NAME1)
                .setClientId2(CLIENT_NAME2)
                .setTimestamp(System.currentTimeMillis())
                .setVolume1(0.0)
                .setVolume2(0.0)
    }
}