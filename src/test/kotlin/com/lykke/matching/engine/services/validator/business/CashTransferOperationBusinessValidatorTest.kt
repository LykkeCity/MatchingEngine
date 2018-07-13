package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import junit.framework.Assert
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashTransferOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashTransferOperationBusinessValidatorTest {

    companion object {
        val CLIENT_NAME1 = "Client1"
        val CLIENT_NAME2 = "Client2"
        val ASSET_ID = "USD"
    }

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator

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

    @Test(expected = ValidationException::class)
    fun testLowBalance() {
        //given
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID, 100.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME1, ASSET_ID, 50.0)
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.overdraftLimit = -70.0

        //when
        try {
            cashTransferOperationBusinessValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            Assert.assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
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
        return cashTransferParser.parse(getMessageWrapper(message)).messageWrapper.context as CashTransferContext
    }
}