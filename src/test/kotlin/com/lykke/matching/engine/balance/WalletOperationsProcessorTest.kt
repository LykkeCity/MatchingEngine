package com.lykke.matching.engine.balance

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.Date
import kotlin.test.assertEquals
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

    @Test
    fun testPreProcessWalletOperations() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC",  0.1)
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        walletOperationsProcessor.preProcess(
                listOf(
                        WalletOperation("1", null, "Client1", "BTC", Date(), -0.5, -0.1),
                        WalletOperation("2", null, "Client2", "ETH", Date(), 2.0, 0.1)

                )
        )

        walletOperationsProcessor.preProcess(
                listOf(WalletOperation("3", null, "Client2", "ETH", Date(), 1.0, 0.2))
        )

        assertFailsWith(BalanceException::class) {
            walletOperationsProcessor.preProcess(
                    listOf(
                            WalletOperation("4", null, "Client1", "BTC", Date(), 0.0, -0.1),
                            WalletOperation("5", null, "Client3", "BTC", Date(), 1.0, 0.0)
                    )
            )
        }
        assertTrue(walletOperationsProcessor.persistBalances(null))
        walletOperationsProcessor.apply().sendNotification("id", "type", "test")

        assertBalance("Client1", "BTC", 0.5, 0.0)
        assertBalance("Client2", "ETH", 3.0, 0.3)
        assertBalance("Client3", "BTC", 0.0, 0.0)

        assertEquals(2, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        assertTrue(setOf("Client1", "Client2").containsAll(balanceUpdateHandlerTest.balanceUpdateQueueNotification.map { it.clientId }))

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(2, balanceUpdate.balances.size)
        assertEquals("id", balanceUpdate.id)
        assertEquals("type", balanceUpdate.type)

        val clientBalanceUpdate1 = balanceUpdate.balances.first { it.id == "Client1" }
        assertNotNull(clientBalanceUpdate1)
        assertEquals("BTC", clientBalanceUpdate1.asset)
        assertEquals(1.0, clientBalanceUpdate1.oldBalance)
        assertEquals(0.5, clientBalanceUpdate1.newBalance)
        assertEquals(0.1, clientBalanceUpdate1.oldReserved)
        assertEquals(0.0, clientBalanceUpdate1.newReserved)

        val clientBalanceUpdate2 = balanceUpdate.balances.first { it.id == "Client2" }
        assertNotNull(clientBalanceUpdate2)
        assertEquals("ETH", clientBalanceUpdate2.asset)
        assertEquals(0.0, clientBalanceUpdate2.oldBalance)
        assertEquals(3.0, clientBalanceUpdate2.newBalance)
        assertEquals(0.0, clientBalanceUpdate2.oldReserved)
        assertEquals(0.3, clientBalanceUpdate2.newReserved)
    }

    @Test
    fun testForceProcessInvalidWalletOperations() {
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null, true)

        walletOperationsProcessor.preProcess(
                listOf(
                        WalletOperation("1", null, "Client1", "BTC", Date(), 0.0, -0.1)
                ), true)

        assertTrue(walletOperationsProcessor.persistBalances(null))
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

    private fun validate(clientId: String, assetId: String, oldBalance: Double, oldReserved: Double, newBalance: Double, newReserved: Double): Boolean {
        return try {
            validateBalanceChange(clientId, assetId, oldBalance, oldReserved, newBalance, newReserved)
            true
        } catch (e: BalanceException) {
            false
        }
    }

    private fun assertBalance(clientId: String, assetId: String, balance: Double, reserved: Double) {
        assertEquals(balance, balancesHolder.getBalance(clientId, assetId))
        assertEquals(reserved, balancesHolder.getReservedBalance(clientId, assetId))
        assertEquals(balance, testWalletDatabaseAccessor.getBalance(clientId, assetId))
        assertEquals(reserved, testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
    }
}