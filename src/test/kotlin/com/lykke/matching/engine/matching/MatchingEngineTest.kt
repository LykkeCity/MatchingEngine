package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import org.apache.log4j.Logger
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue

class MatchingEngineTest {

    private val testDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor, 60000))
    private lateinit var balancesHolder: BalancesHolder
    private lateinit var genericService: GenericLimitOrderService
    private lateinit var matchingEngine: MatchingEngine

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))

        initService()
    }

    @Test
    fun testReservedLimitVolume() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.00952774, price = 10495.66))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.00947867, price = 10550.0, reservedVolume = 0.00947867))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client3", assetId = "BTCUSD", volume = -100.0, straight = false)
        val orderBook = getOrderBook("BTCUSD", false)
        matchingEngine.match(marketOrder, orderBook)

        assertEquals(0.00947867, orderBook.first { it.clientId == "Client1" }.reservedLimitVolume)
    }

    private fun getOrderBook(assetPairId: String, isBuySide: Boolean): PriorityBlockingQueue<NewLimitOrder> =
            genericService.getOrderBook(assetPairId).getOrderBook(isBuySide)

    private fun initService() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, setOf())
        genericService = GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, emptySet())
        matchingEngine = MatchingEngine(Logger.getLogger(MatchingEngineTest::class.java.name), genericService, assetsHolder, assetsPairsHolder, balancesHolder)
    }

}