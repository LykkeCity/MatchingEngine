package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.SingleLimitOrderService
import org.mockito.Mockito
import org.springframework.context.ApplicationEventPublisher
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {
    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val walletDatabaseAccessor = TestWalletDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestConfigDatabaseAccessor
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor
    protected lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService
    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    protected lateinit var genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory
    protected lateinit var limitOrdersProcessorFactory: LimitOrdersProcessorFactory

    protected lateinit var assetsHolder: AssetsHolder
    protected lateinit var balancesHolder: BalancesHolder
    protected lateinit var assetsPairsHolder: AssetsPairsHolder
    protected lateinit var assetCache: AssetsCache

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected val applicationEventPublicher = Mockito.mock(ApplicationEventPublisher::class.java)

    protected lateinit var tradesInfoQueue: LinkedBlockingQueue<TradeInfo>
    protected lateinit var quotesNotificationQueue: LinkedBlockingQueue<QuotesUpdate>
    protected lateinit var clientsLimitOrdersQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var trustedClientsLimitOrdersQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var lkkTradesQueue: LinkedBlockingQueue<List<LkkTrade>>
    protected lateinit var orderBookQueue: LinkedBlockingQueue<OrderBook>
    protected lateinit var rabbitOrderBookQueue: LinkedBlockingQueue<JsonSerializable>

    open fun initServices() {
        walletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        walletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        walletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))
        walletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 6))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
        testSettingsDatabaseAccessor.addTrustedClient("Client3")

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("ETHBTC", "ETH", "BTC", 5))


        testConfigDatabaseAccessor = TestConfigDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testConfigDatabaseAccessor, 60000)

        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesHolder = BalancesHolder(walletDatabaseAccessor,
                assetsHolder,
                applicationEventPublicher,
                applicationSettingsCache)

        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)


        tradesInfoQueue = LinkedBlockingQueue()
        quotesNotificationQueue = LinkedBlockingQueue()

        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                tradesInfoQueue,
                quotesNotificationQueue,
                applicationSettingsCache)

        clientsLimitOrdersQueue = LinkedBlockingQueue()

        stopOrderDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService)

        trustedClientsLimitOrdersQueue = LinkedBlockingQueue()
        lkkTradesQueue = LinkedBlockingQueue()
        orderBookQueue = LinkedBlockingQueue()
        rabbitOrderBookQueue = LinkedBlockingQueue()
        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder,
                genericLimitOrderService, applicationSettingsCache,
                trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue,
                lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue)

        genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                clientsLimitOrdersQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache)

        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)
    }
}