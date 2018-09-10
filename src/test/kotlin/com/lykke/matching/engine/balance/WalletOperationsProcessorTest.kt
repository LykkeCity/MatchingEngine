package com.lykke.matching.engine.balance

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.Date
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (WalletOperationsProcessorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WalletOperationsProcessorTest : AbstractTest() {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 6))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @Test
    fun testPreProcessWalletOperations() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC",  0.1)
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        walletOperationsProcessor.preProcess(
                listOf(
                        WalletOperation("1", null, "Client1", "BTC", Date(), BigDecimal.valueOf( -0.5), BigDecimal.valueOf(-0.1)),
                        WalletOperation("2", null, "Client2", "ETH", Date(), BigDecimal.valueOf(2.0), BigDecimal.valueOf(0.1))

                )
        )

        walletOperationsProcessor.preProcess(
                listOf(WalletOperation("3", null, "Client2", "ETH", Date(), BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.2)))
        )

        assertFailsWith(BalanceException::class) {
            walletOperationsProcessor.preProcess(
                    listOf(
                            WalletOperation("4", null, "Client1", "BTC", Date(), BigDecimal.ZERO, BigDecimal.valueOf(-0.1)),
                            WalletOperation("5", null, "Client3", "BTC", Date(), BigDecimal.valueOf(1.0), BigDecimal.ZERO)
                    )
            )
        }
        assertTrue(walletOperationsProcessor.persistBalances(null, null))
        walletOperationsProcessor.apply().sendNotification("id", "type", "test")

        assertBalance("Client1", "BTC", 0.5, 0.0)
        assertBalance("Client2", "ETH", 3.0, 0.3)
        assertBalance("Client3", "BTC", 0.0, 0.0)

        assertEquals(2, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        assertTrue(setOf("Client1", "Client2").containsAll(balanceUpdateHandlerTest.balanceUpdateNotificationQueue.map { it.clientId }))

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(2, balanceUpdate.balances.size)
        assertEquals("id", balanceUpdate.id)
        assertEquals("type", balanceUpdate.type)

        val clientBalanceUpdate1 = balanceUpdate.balances.first { it.id == "Client1" }
        assertNotNull(clientBalanceUpdate1)
        assertEquals("BTC", clientBalanceUpdate1.asset)
        assertEquals(BigDecimal.valueOf(1.0), clientBalanceUpdate1.oldBalance)
        assertEquals(BigDecimal.valueOf(0.5), clientBalanceUpdate1.newBalance)
        assertEquals(BigDecimal.valueOf(0.1), clientBalanceUpdate1.oldReserved)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdate1.newReserved)

        val clientBalanceUpdate2 = balanceUpdate.balances.first { it.id == "Client2" }
        assertNotNull(clientBalanceUpdate2)
        assertEquals("ETH", clientBalanceUpdate2.asset)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdate2.oldBalance)
        assertEquals(BigDecimal.valueOf(3.0), clientBalanceUpdate2.newBalance)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdate2.oldReserved)
        assertEquals(BigDecimal.valueOf(0.3), clientBalanceUpdate2.newReserved)
    }

    @Test
    fun testForceProcessInvalidWalletOperations() {
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        walletOperationsProcessor.preProcess(
                listOf(
                        WalletOperation("1", null, "Client1", "BTC", Date(), BigDecimal.ZERO, BigDecimal.valueOf(-0.1))
                ), true)

        assertTrue(walletOperationsProcessor.persistBalances(null, null))
        walletOperationsProcessor.apply().sendNotification("id", "type","test")

        assertBalance("Client1", "BTC", 0.0, -0.1)
    }

    @Test
    fun testValidation() {
        assertTrue(validate("Client", "Asset", 0.0, 0.0, 1.0, 0.0))
        assertTrue(validate("Client", "Asset", -1.0, 0.0, -1.0, 0.0))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.0, -1.0))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.0, -0.9))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.2, -1.0))
        assertTrue(validate("Client", "Asset", 1.0, 2.0, 1.0, 2.0))
        assertTrue(validate("Client", "Asset", 1.0, 2.0, 1.0, 1.9))
        assertTrue(validate("Client", "Asset", 0.05, 0.09, 0.0, 0.04))

        assertFalse(validate("Client", "Asset", 0.0, 0.0, -1.0, -1.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, -1.0, 0.0))
        assertFalse(validate("Client", "Asset", -1.0, 0.0, -1.1, -0.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, 0.0, -1.0))
        assertFalse(validate("Client", "Asset", 0.0, -1.0, -0.1, -1.0))
        assertFalse(validate("Client", "Asset", 0.0, -1.0, 0.0, -1.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, 1.0, 2.0))
        assertFalse(validate("Client", "Asset", 1.0, 2.0, 1.0, 2.1))
        assertFalse(validate("Client", "Asset", 1.0, 2.0, -0.1, 0.9))
    }

    @Test
    fun testTrustedClientReservedOperations() {
        testConfigDatabaseAccessor.addTrustedClient("TrustedClient1")
        testConfigDatabaseAccessor.addTrustedClient("TrustedClient2")
        applicationSettingsCache.update()

        testBalanceHolderWrapper.updateBalance("TrustedClient1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient2", "EUR", 1.0)
        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        val now = Date()
        walletOperationsProcessor.preProcess(listOf(
                WalletOperation("1", "1", "TrustedClient1", "BTC", now, BigDecimal.ZERO, BigDecimal.valueOf(0.1)),
                WalletOperation("2", "2", "TrustedClient2", "ETH", now, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1))))

        val clientBalanceUpdates = walletOperationsProcessor.getClientBalanceUpdates()
        assertEquals(1, clientBalanceUpdates.size)
        assertEquals("ETH", clientBalanceUpdates.single().asset)
        assertEquals("TrustedClient2", clientBalanceUpdates.single().id)
        assertEquals(BigDecimal.valueOf(0.1), clientBalanceUpdates.single().newBalance)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdates.single().newReserved)

        walletOperationsProcessor.sendNotification("id", "type", "messageId")

        assertEquals(1, balanceUpdateHandlerTest.balanceUpdateNotificationQueue.size)
        assertEquals("TrustedClient2", balanceUpdateHandlerTest.balanceUpdateNotificationQueue.single().clientId)
    }

    @Test
    fun testNotChangedBalance() {
        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        val now = Date()
        walletOperationsProcessor.preProcess(listOf(
                WalletOperation("1", "1", "Client1", "BTC", now, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1)),
                WalletOperation("1", "1", "Client1", "BTC", now, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1)),
                WalletOperation("2", "2", "Client2", "BTC", now, BigDecimal.valueOf(0.00000001), BigDecimal.ZERO),
                WalletOperation("3", "3", "Client2", "BTC", now, BigDecimal.valueOf(0.00000001),  BigDecimal.valueOf(0.00000001)),
                WalletOperation("4", "4", "Client2", "BTC", now, BigDecimal.valueOf(-0.00000002),  BigDecimal.valueOf(-0.00000001)),
                WalletOperation("5", "5", "Client3", "BTC", now, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1))))

        walletOperationsProcessor.preProcess(listOf(
                WalletOperation("6", "6", "Client3", "BTC", now, BigDecimal.valueOf(-0.1), BigDecimal.valueOf(-0.1))
        ))

        val clientBalanceUpdates = walletOperationsProcessor.getClientBalanceUpdates()
        assertEquals(1, clientBalanceUpdates.size)
        assertEquals("BTC", clientBalanceUpdates.single().asset)
        assertEquals("Client1", clientBalanceUpdates.single().id)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdates.single().oldBalance)
        assertEquals(BigDecimal.ZERO, clientBalanceUpdates.single().oldReserved)
        assertEquals(BigDecimal.valueOf(0.2), clientBalanceUpdates.single().newBalance)
        assertEquals(BigDecimal.valueOf(0.2), clientBalanceUpdates.single().newReserved)

        walletOperationsProcessor.sendNotification("id", "type", "messageId")

        assertEquals(1, balanceUpdateHandlerTest.balanceUpdateNotificationQueue.size)
        assertEquals("Client1", balanceUpdateHandlerTest.balanceUpdateNotificationQueue.single().clientId)
    }

    private fun validate(clientId: String, assetId: String, oldBalance: Double, oldReserved: Double, newBalance: Double, newReserved: Double): Boolean {
        return try {
            validateBalanceChange(clientId, assetId, BigDecimal.valueOf(oldBalance), BigDecimal.valueOf(oldReserved),
                    BigDecimal.valueOf(newBalance),BigDecimal.valueOf(newReserved))
            true
        } catch (e: BalanceException) {
            false
        }
    }

    private fun assertBalance(clientId: String, assetId: String, balance: Double, reserved: Double) {
        assertEquals(BigDecimal.valueOf(balance), balancesHolder.getBalance(clientId, assetId))
        assertEquals(BigDecimal.valueOf(reserved), balancesHolder.getReservedBalance(clientId, assetId))
        assertEquals(BigDecimal.valueOf(balance), testWalletDatabaseAccessor.getBalance(clientId, assetId))
        assertEquals(BigDecimal.valueOf(reserved), testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
    }
}