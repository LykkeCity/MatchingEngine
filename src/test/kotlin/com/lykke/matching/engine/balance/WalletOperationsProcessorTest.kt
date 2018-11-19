package com.lykke.matching.engine.balance

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
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
                        WalletOperation("Client1", "BTC", BigDecimal.valueOf( -0.5), BigDecimal.valueOf(-0.1)),
                        WalletOperation("Client2", "ETH", BigDecimal.valueOf(2.0), BigDecimal.valueOf(0.1))

                )
        )

        walletOperationsProcessor.preProcess(
                listOf(WalletOperation("Client2", "ETH", BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.2)))
        )

        assertFailsWith(BalanceException::class) {
            walletOperationsProcessor.preProcess(
                    listOf(
                            WalletOperation("Client1", "BTC", BigDecimal.ZERO, BigDecimal.valueOf(-0.1)),
                            WalletOperation("Client3", "BTC", BigDecimal.valueOf(1.0), BigDecimal.ZERO)
                    )
            )
        }
        assertTrue(walletOperationsProcessor.persistBalances(null, null, null, null))
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
                        WalletOperation("Client1", "BTC", BigDecimal.ZERO, BigDecimal.valueOf(-0.1))
                ), true)

        assertTrue(walletOperationsProcessor.persistBalances(null, null, null, null))
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