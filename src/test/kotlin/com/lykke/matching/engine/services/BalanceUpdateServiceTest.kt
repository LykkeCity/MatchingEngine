package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue

class BalanceUpdateServiceTest {

    companion object {
        private val DELTA = 1e-15
    }

    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private lateinit var balancesHolder: BalancesHolder

    @Before
    fun setUp() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 1000.0))
        initBalanceHolder()
    }

    private fun initBalanceHolder() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue, setOf("trustedClient"))
    }

    @Test
    fun testUpdateBalance() {
        val updateService = BalanceUpdateService(balancesHolder)
        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "Client1", "Asset1", 999.0, 1000.0, 0.0, 0.0)
    }

    @Test
    fun testUpdateBalanceWithAnotherAssetBalance() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset2", 2000.0, 500.0))
        initBalanceHolder()

        val updateService = BalanceUpdateService(balancesHolder)
        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "Client1", "Asset1", 999.0, 1000.0, 0.0, 0.0)
        assertUpdateResult("Client1", "Asset2", 2000.0, 500.0)
    }

    @Test
    fun testUpdateBalanceOfNewClient() {
        val updateService = BalanceUpdateService(balancesHolder)
        updateService.processMessage(buildBalanceUpdateWrapper("ClientNew", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.BALANCE_UPDATE, "ClientNew", "Asset1", 999.0, 0.0, 0.0, 0.0)
    }

    @Test
    fun testUpdateBalanceLowerThanResolved() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 1000.0, 1000.0))
        initBalanceHolder()

        val updateService = BalanceUpdateService(balancesHolder)
        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertUnsuccessfulUpdate("Client1", "Asset1", 1000.0, 1000.0)
    }

    @Test
    fun testUpdateReservedBalance() {
        val updateService = ReservedBalanceUpdateService(balancesHolder)
        updateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.RESERVED_BALANCE_UPDATE, "Client1", "Asset1", 1000.0, 1000.0, 999.0, 0.0)
    }

    @Test
    fun testUpdateTrustedClientReservedBalance() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("trustedClient", "Asset1", 10.0, 0.0))
        initBalanceHolder()

        val updateService = ReservedBalanceUpdateService(balancesHolder)
        updateService.processMessage(buildReservedBalanceUpdateWrapper("trustedClient", "Asset1", 9.0))

        assertSuccessfulUpdate(MessageType.RESERVED_BALANCE_UPDATE, "trustedClient", "Asset1", 10.0, 10.0, 9.0, 0.0)
    }

    @Test
    fun testUpdateReservedBalanceWithAnotherAssetBalance() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset2", 2000.0, 500.0))
        initBalanceHolder()

        val updateService = ReservedBalanceUpdateService(balancesHolder)
        updateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        assertSuccessfulUpdate(MessageType.RESERVED_BALANCE_UPDATE, "Client1", "Asset1", 1000.0, 1000.0, 999.0, 0.0)
        assertUpdateResult("Client1", "Asset2", 2000.0, 500.0)
    }

    @Test
    fun testUpdateReservedBalanceOfNewClient() {
        val updateService = ReservedBalanceUpdateService(balancesHolder)
        updateService.processMessage(buildReservedBalanceUpdateWrapper("ClientNew", "Asset1", 999.0))

        assertUnsuccessfulUpdate("ClientNew", "Asset1", 0.0, 0.0)
    }

    @Test
    fun testUpdateReservedBalanceHigherThanBalance() {
        val updateService = ReservedBalanceUpdateService(balancesHolder)
        updateService.processMessage(buildReservedBalanceUpdateWrapper("Client1", "Asset1", 1001.0))

        assertUnsuccessfulUpdate("Client1", "Asset1", 1000.0, 0.0)
    }

    private fun assertSuccessfulUpdate(messageType: MessageType, clientId: String, assetId: String, balance: Double, oldBalance: Double, reserved: Double, oldReserved: Double) {
        assertUpdateResult(clientId, assetId, balance, reserved)

        assertEquals(1, balanceNotificationQueue.size)
        val notification = balanceNotificationQueue.poll()
        assertEquals(clientId, notification.clientId)

        assertEquals(1, balanceUpdateQueue.size)
        val balanceUpdate = balanceUpdateQueue.poll() as BalanceUpdate
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
        assertEquals(0, balanceNotificationQueue.size)
        assertEquals(0, balanceUpdateQueue.size)
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