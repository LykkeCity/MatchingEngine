package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.BalanceUpdateValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (BalanceUpdateValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class BalanceUpdateValidatorTest {

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
            testBackOfficeDatabaseAccessor.addAsset(Asset(ReservedCashInOutOperationValidatorTest.ASSET_ID, 2))
            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var balanceUpdateValidator: BalanceUpdateValidator

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun init() {
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 100.0)
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 100.0)
    }

    @Test(expected = ValidationException::class)
    fun testInvalidBalance() {
        //given
        val message = getDefaultBalanceUpdateBuilder()
                .setAmount(99.0)
                .build()

        //when
        try {
            balanceUpdateValidator.performValidation(message)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.BALANCE_LOWER_THAN_RESERVED, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidAmountAccuracy() {
        //given
        val message = getDefaultBalanceUpdateBuilder()
                .setAmount(199.011)
                .build()

        //when
        try {
            balanceUpdateValidator.performValidation(message)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //given
        val message = getDefaultBalanceUpdateBuilder()
                .build()

        //when
            balanceUpdateValidator.performValidation(message)
    }

    private fun getDefaultBalanceUpdateBuilder(): ProtocolMessages.BalanceUpdate.Builder {
        return ProtocolMessages.BalanceUpdate.newBuilder()
                .setUid("test")
                .setClientId(CLIENT_NAME)
                .setAssetId(ASSET_ID)
                .setAmount(1000.0)
    }
}