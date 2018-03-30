package com.lykke.matching.engine

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.CashInOutOperationService
import com.lykke.matching.engine.services.CashTransferOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.ReservedBalanceUpdateService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import org.springframework.beans.factory.annotation.Autowired
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractTest {
    @Autowired
    private lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

    @Autowired
    lateinit var balancesHolder: BalancesHolder

    @Autowired
    protected lateinit var testWalletDatabaseAccessor: TestWalletDatabaseAccessor

    @Autowired
    protected lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var assetsCache: AssetsCache

    @Autowired
    protected lateinit var assetsHolder: AssetsHolder

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    protected val testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected val testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
    protected val testCashOperationsDatabaseAccessor = TestCashOperationsDatabaseAccessor()

    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    protected val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    protected val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    protected val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val clientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    protected val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val cashInOutQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val reservedCashInOutQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val rabbitTransferQueue = LinkedBlockingQueue<JsonSerializable>()

    protected val assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
    protected val assetsPairsHolder = AssetsPairsHolder(assetPairsCache)

    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    protected lateinit var feeProcessor: FeeProcessor

    protected lateinit var cashInOutOperationService: CashInOutOperationService
    protected lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService
    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService
    protected lateinit var balanceUpdateService: BalanceUpdateService
    protected lateinit var reservedBalanceUpdateService: ReservedBalanceUpdateService
    protected lateinit var limitOrderCancelService: LimitOrderCancelService
    protected lateinit var cashTransferOperationsService: CashTransferOperationService

    protected open fun initServices() {
        assetsCache.update()
        assetPairsCache.update()
        balancesHolder.reload()
        applicationSettingsCache.update()

        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)

        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)

        cashTransferOperationsService = CashTransferOperationService(balancesHolder, assetsHolder, applicationSettingsCache, testCashOperationsDatabaseAccessor, rabbitTransferQueue, FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService))
        balanceUpdateService = BalanceUpdateService(balancesHolder)
        reservedBalanceUpdateService = ReservedBalanceUpdateService(balancesHolder)
        cashInOutOperationService = CashInOutOperationService(testWalletDatabaseAccessor, assetsHolder, balancesHolder, applicationSettingsCache, cashInOutQueue, feeProcessor)
        reservedCashInOutOperationService = ReservedCashInOutOperationService(assetsHolder, balancesHolder, reservedCashInOutQueue)
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache, lkkTradesQueue)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, lkkTradesQueue)
        limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, clientsLimitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, orderBookQueue, rabbitOrderBookQueue)
    }

    protected fun clearMessageQueues() {
        balanceUpdateHandlerTest.clear()
        quotesNotificationQueue.clear()
        tradesInfoQueue.clear()
        orderBookQueue.clear()
        rabbitOrderBookQueue.clear()
        trustedClientsLimitOrdersQueue.clear()
        clientsLimitOrdersQueue.clear()
        lkkTradesQueue.clear()
        rabbitSwapQueue.clear()
    }
}