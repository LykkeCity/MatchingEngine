package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.CashOutEvent
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.NumberUtils
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
import java.math.BigDecimal
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import java.util.concurrent.BlockingQueue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedVolumesRecalculatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedVolumesRecalculatorTest {

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    protected lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Autowired
    private
    lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    @Autowired
    lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    lateinit var reservedVolumesRecalculator: ReservedVolumesRecalculator

    @Autowired
    lateinit var reservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor

    @Autowired
    lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

    @Autowired
    lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Before
    fun setUp() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "trustedClient", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "1", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.4))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "2", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.3))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 1.0))

        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(uid = "3", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 9000.0, lowerPrice = 9900.0, reservedVolume = 990.0))
        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 10000.0, lowerPrice = 10900.0))

        testBalanceHolderWrapper.updateBalance("trustedClient", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("trustedClient", "BTC", 2.0)
        // negative reserved balance
        testBalanceHolderWrapper.updateBalance("trustedClient2", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("trustedClient2", "BTC", -0.001)

        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", -0.001)


        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 1.0)

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 2.0)

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 3.0)


        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 0.0)

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC", 1.0)

        testBalanceHolderWrapper.updateBalance("Client2", "USD", 990.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 1.0)

        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "trustedClient", "trustedClient", true)
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "trustedClient2", "trustedClient2", true)
    }

    @Test
    fun testRecalculate() {
        reservedVolumesRecalculator.recalculate()

        val testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("trustedClient", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("trustedClient2", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.5), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(0.7), testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertEquals(BigDecimal.valueOf(2080.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(7, reservedVolumesDatabaseAccessor.corrections.size)
        assertEquals("1,2", reservedVolumesDatabaseAccessor.corrections.first { NumberUtils.equalsIgnoreScale(it.newReserved, BigDecimal.valueOf(0.7)) }.orderIds)
        assertEquals("3,4", reservedVolumesDatabaseAccessor.corrections.first { NumberUtils.equalsIgnoreScale(it.newReserved, BigDecimal.valueOf(2080.0)) }.orderIds)

        assertEquals(1, balanceUpdateHandlerTest.balanceUpdateQueue.size)
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(7, balanceUpdate.balances.size)
        assertBalanceUpdateNotification("trustedClient", "BTC", 10.0, 2.0, 0.0, balanceUpdate.balances)
        assertBalanceUpdateNotification("trustedClient2", "BTC", 1.0, -0.001, 0.0, balanceUpdate.balances)
        assertBalanceUpdateNotification("Client3", "BTC", 0.0, -0.001, 0.0, balanceUpdate.balances)
        assertBalanceUpdateNotification("Client1", "BTC", 10.0, 2.0, 0.5, balanceUpdate.balances)
        assertBalanceUpdateNotification("Client1", "USD", 10.0, 1.0, 0.0, balanceUpdate.balances)
        assertBalanceUpdateNotification("Client1", "EUR", 10.0, 3.0, 0.7, balanceUpdate.balances)
        assertBalanceUpdateNotification("Client2", "USD", 990.0, 1.0, 2080.0, balanceUpdate.balances)

        assertEquals(7, clientsEventsQueue.size)
        assertEvent(false, "trustedClient", "BTC", "10", "2", "0", clientsEventsQueue)
        assertEvent(true, "trustedClient2", "BTC", "1", "-0.001", "0", clientsEventsQueue)
        assertEvent(true, "Client3", "BTC", "0", "-0.001", "0", clientsEventsQueue)
        assertEvent(false, "Client1", "BTC", "10", "2", "0.5", clientsEventsQueue)
        assertEvent(false, "Client1", "USD", "10", "1", "0", clientsEventsQueue)
        assertEvent(false, "Client1", "EUR", "10", "3", "0.7", clientsEventsQueue)
        assertEvent(true, "Client2", "USD", "990", "1", "2080", clientsEventsQueue)
    }

    private fun assertBalanceUpdateNotification(clientId: String, assetId: String, balance: Double, oldReserved: Double, newReserved: Double, balanceUpdates: Collection<ClientBalanceUpdate>) {
        val balanceUpdate = balanceUpdates.single { it.id == clientId && it.asset == assetId }
        val message = "Client $clientId, assetId $assetId"
        assertEquals(BigDecimal.valueOf(balance), balanceUpdate.oldBalance, message)
        assertEquals(BigDecimal.valueOf(balance), balanceUpdate.newBalance, message)
        assertEquals(BigDecimal.valueOf(oldReserved), balanceUpdate.oldReserved, message)
        assertEquals(BigDecimal.valueOf(newReserved), balanceUpdate.newReserved, message)
    }

    private fun assertEvent(isCashIn: Boolean, clientId: String, assetId: String, balance: String, oldReserved: String, newReserved: String, events: Collection<Event<*>>) {
        val event = events.single {
            isCashIn && it is CashInEvent && it.cashIn.walletId == clientId && it.cashIn.assetId == assetId
                    || !isCashIn && it is CashOutEvent && it.cashOut.walletId == clientId && it.cashOut.assetId == assetId
        }
        val message = "Client $clientId, assetId $assetId"
        val balanceUpdate = if (isCashIn) {
            event as CashInEvent
            assertEquals(1, event.balanceUpdates.size)
            event.balanceUpdates.first()
        } else {
            event as CashOutEvent
            assertEquals(1, event.balanceUpdates.size)
            event.balanceUpdates.first()
        }
        assertEquals(balance, balanceUpdate.oldBalance, message)
        assertEquals(balance, balanceUpdate.newBalance, message)
        assertEquals(oldReserved, balanceUpdate.oldReserved, message)
        assertEquals(newReserved, balanceUpdate.newReserved, message)
    }

}