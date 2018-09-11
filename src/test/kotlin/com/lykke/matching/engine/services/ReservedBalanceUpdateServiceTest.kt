package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import org.junit.Assert.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedBalanceUpdateServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedBalanceUpdateServiceTest : AbstractTest() {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset1", 2))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset1", 1000.0)
        initServices()
    }

    @Test
    fun testUpdateReservedBalance() {
        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.RESERVED_BALANCE_UPDATE, "Client1", "Asset1", 1000.0, 1000.0, 999.0, 0.0)
    }

    @Test
    fun testUpdateReservedBalanceWithAnotherAssetBalance() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset2", 2000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset2",  500.0)
        initServices()

        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.RESERVED_BALANCE_UPDATE, "Client1", "Asset1", 1000.0, 1000.0, 999.0, 0.0)
        assertUpdateResult("Client1", "Asset2", BigDecimal.valueOf( 2000.0), BigDecimal.valueOf(500.0))
    }

    @Test
    fun testUpdateReservedBalanceOfNewClient() {
        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("ClientNew", "Asset1", 999.0))

        assertUnsuccessfulUpdate("ClientNew", "Asset1", BigDecimal.ZERO, BigDecimal.ZERO)
    }

    @Test
    fun testUpdateReservedBalanceHigherThanBalance() {
        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 1001.0))

        assertUnsuccessfulUpdate("Client1", "Asset1",  BigDecimal.valueOf(1000.0), BigDecimal.ZERO)
    }

    private fun assertSuccessfulUpdate(messageType: MessageType, clientId: String, assetId: String,
                                       balance: Double, oldBalance: Double, reserved: Double, oldReserved: Double) {
        assertUpdateResult(clientId, assetId, BigDecimal.valueOf(balance), BigDecimal.valueOf(reserved))

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        val notification = balanceUpdateHandlerTest.balanceUpdateNotificationQueue.poll()
        assertEquals(clientId, notification.clientId)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(messageType.name, balanceUpdate.type)

        assertEquals(1, balanceUpdate.balances.size)
        val clientBalanceUpdate = balanceUpdate.balances.first()
        assertEquals(BigDecimal.valueOf(oldBalance), clientBalanceUpdate.oldBalance)
        assertEquals(BigDecimal.valueOf(balance), clientBalanceUpdate.newBalance)
        assertEquals(BigDecimal.valueOf(oldReserved), clientBalanceUpdate.oldReserved)
        assertEquals(BigDecimal.valueOf(reserved), clientBalanceUpdate.newReserved)
    }

    private fun assertUnsuccessfulUpdate(clientId: String, assetId: String, balance: BigDecimal, reserved: BigDecimal) {
        assertUpdateResult(clientId, assetId, balance, reserved)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
    }

    private fun assertUpdateResult(clientId: String, assetId: String, balance: BigDecimal, reserved: BigDecimal) {
        val dbBalance = testWalletDatabaseAccessor.getBalance(clientId, assetId)
        val dbReserved = testWalletDatabaseAccessor.getReservedBalance(clientId, assetId)
        assertEquals(balance, dbBalance)
        assertEquals(reserved, dbReserved)

        val cacheBalance = balancesHolder.getBalance(clientId, assetId)
        val cacheReserved = balancesHolder.getReservedBalance(clientId, assetId)
        assertEquals(cacheBalance, dbBalance)
        assertEquals(cacheReserved, dbReserved)
    }

    private fun buildReservedBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper("Test", MessageType.RESERVED_BALANCE_UPDATE.type, ProtocolMessages.ReservedBalanceUpdate.newBuilder()
                .setUid("123")
                .setClientId(clientId)
                .setAssetId(assetId)
                .setReservedAmount(amount).build().toByteArray(), null)
    }
}