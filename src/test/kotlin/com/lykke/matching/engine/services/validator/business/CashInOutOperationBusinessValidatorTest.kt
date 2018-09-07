package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validator.CashOperationValidatorTest
import com.lykke.matching.engine.services.validator.input.CashInOutOperationInputValidatorTest
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashInOutOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashInOutOperationBusinessValidatorTest {

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
            testBackOfficeDatabaseAccessor.addAsset(Asset(CashInOutOperationInputValidatorTest.ASSET_ID, 2))
            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator

    @Autowired
    private lateinit var cashInOutContextInitializer: CashInOutContextParser

    @Test(expected = ValidationException::class)
    fun testBalanceValid() {
        //given
        testBalanceHolderWrapper.updateBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 500.0)
        testBalanceHolderWrapper.updateReservedBalance(CashOperationValidatorTest.CLIENT_NAME, CashOperationValidatorTest.ASSET_ID, 250.0)
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.volume = -300.0

        //when
        try {
            cashInOutOperationBusinessValidator.performValidation(getContext(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
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

    private fun getContext(cashInOutOperation: ProtocolMessages.CashInOutOperation): CashInOutContext {
        return cashInOutContextInitializer.parse(getMessageWrapper(cashInOutOperation)).messageWrapper.context as CashInOutContext
    }
}