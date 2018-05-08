package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class BalanceUpdateServiceTest: AbstractTest() {
    companion object {
        private const val DELTA = 1e-15
    }

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset1", 1000.0)
        initServices()
    }

    @Test
    fun testUpdateBalance() {
        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "Client1", "Asset1", 999.0, 1000.0, 0.0, 0.0)
    }

    @Test
    fun testUpdateBalanceWithAnotherAssetBalance() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset2", 2000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset2",  500.0)
        initServices()

        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "Client1", "Asset1", 999.0, 1000.0, 0.0, 0.0)
        assertUpdateResult("Client1", "Asset2", 2000.0, 500.0)
    }

    @Test
    fun testUpdateBalanceOfNewClient() {
        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("ClientNew", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "ClientNew", "Asset1", 999.0, 0.0, 0.0, 0.0)
    }

    @Test
    fun testUpdateBalanceLowerThanResolved() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset1", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset1",  1000.0)
        initServices()

        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertUnsuccessfulUpdate("Client1", "Asset1", 1000.0, 1000.0)
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
        assertUpdateResult("Client1", "Asset2", 2000.0, 500.0)
    }

    @Test
    fun testUpdateReservedBalanceOfNewClient() {
        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("ClientNew", "Asset1", 999.0))

        assertUnsuccessfulUpdate("ClientNew", "Asset1", 0.0, 0.0)
    }

    @Test
    fun testUpdateReservedBalanceHigherThanBalance() {
        reservedBalanceUpdateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 1001.0))

        assertUnsuccessfulUpdate("Client1", "Asset1", 1000.0, 0.0)
    }

    private fun assertSuccessfulUpdate(messageType: MessageType, clientId: String, assetId: String, balance: Double, oldBalance: Double, reserved: Double, oldReserved: Double) {
        assertUpdateResult(clientId, assetId, balance, reserved)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        val notification = balanceUpdateHandlerTest.balanceUpdateQueueNotification.poll()
        assertEquals(clientId, notification.clientId)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll()
        assertEquals(messageType.name, balanceUpdate.type)

        assertEquals(1, balanceUpdate.balances.size)
        val clientBalanceUpdate = balanceUpdate.balances.first()
        assertEquals(oldBalance, clientBalanceUpdate.oldBalance, DELTA)
        assertEquals(balance, clientBalanceUpdate.newBalance, DELTA)
        assertEquals(oldReserved, clientBalanceUpdate.oldReserved, DELTA)
        assertEquals(reserved, clientBalanceUpdate.newReserved, DELTA)
    }

    private fun assertUnsuccessfulUpdate(clientId: String, assetId: String, balance: Double, reserved: Double) {
        assertUpdateResult(clientId, assetId, balance, reserved)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
    }

    private fun assertUpdateResult(clientId: String, assetId: String, balance: Double, reserved: Double) {
        val dbBalance = testWalletDatabaseAccessor.getBalance(clientId, assetId)
        val dbReserved = testWalletDatabaseAccessor.getReservedBalance(clientId, assetId)
        assertEquals(balance, dbBalance, DELTA)
        assertEquals(reserved, dbReserved, DELTA)

        val cacheBalance = balancesHolder.getBalance(clientId, assetId)
        val cacheReserved = balancesHolder.getReservedBalance(clientId, assetId)
        assertEquals(cacheBalance, dbBalance, DELTA)
        assertEquals(cacheReserved, dbReserved, DELTA)
    }

    private fun buildBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper("Test", MessageType.BALANCE_UPDATE.type, ProtocolMessages.BalanceUpdate.newBuilder()
                .setUid("123")
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount).build().toByteArray(), null)
    }

    private fun buildReservedBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper("Test", MessageType.RESERVED_BALANCE_UPDATE.type, ProtocolMessages.ReservedBalanceUpdate.newBuilder()
                .setUid("123")
                .setClientId(clientId)
                .setAssetId(assetId)
                .setReservedAmount(amount).build().toByteArray(), null)
    }
}