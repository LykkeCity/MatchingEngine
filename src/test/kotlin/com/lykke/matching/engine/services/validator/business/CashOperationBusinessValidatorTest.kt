package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.impl.CashOperationContextParser
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.business.CashOperationBusinessValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashOperationBusinessValidatorTest {
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
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var cashOperationBusinessValidator: CashOperationBusinessValidator

    @Autowired
    private lateinit var cashOperationContextParser: CashOperationContextParser



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
            cashOperationBusinessValidator.performValidation(getContext(cashOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    fun getContext(cashOperationMessages: ProtocolMessages.CashOperation): CashOperationContext {
        return cashOperationContextParser
                .parse(MessageWrapper("test", 1, cashOperationMessages.toByteArray(), null))
                .messageWrapper
                .context as CashOperationContext

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