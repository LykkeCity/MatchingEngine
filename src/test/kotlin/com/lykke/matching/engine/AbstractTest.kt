package com.lykke.matching.engine

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import java.util.concurrent.LinkedBlockingQueue
import org.springframework.beans.factory.annotation.Autowired

abstract class AbstractTest {
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
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    protected val testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected val stopOrderDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
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
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    protected lateinit var feeProcessor: FeeProcessor

    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService
    protected lateinit var cashInOutOperationService: CashInOutOperationService
    protected lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService
    protected lateinit var cashTransferOperationsService: CashTransferOperationService
    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService
    protected lateinit var balanceUpdateService: BalanceUpdateService
    protected lateinit var reservedBalanceUpdateService: ReservedBalanceUpdateService
    protected lateinit var limitOrderCancelService: LimitOrderCancelService

    protected open fun initServices() {
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()

        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService)

        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
        val genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache, lkkTradesQueue)

        cashTransferOperationsService = CashTransferOperationService(balancesHolder, assetsHolder, applicationSettingsCache, testCashOperationsDatabaseAccessor, rabbitTransferQueue, FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService))
        minVolumeOrderCanceller = MinVolumeOrderCanceller(testDictionariesDatabaseAccessor, assetsPairsHolder, balancesHolder, genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, genericLimitOrderProcessorFactory)
        balanceUpdateService = BalanceUpdateService(balancesHolder)
        reservedBalanceUpdateService = ReservedBalanceUpdateService(balancesHolder)
        cashInOutOperationService = CashInOutOperationService(testWalletDatabaseAccessor, assetsHolder, balancesHolder, applicationSettingsCache, cashInOutQueue, feeProcessor)
        reservedCashInOutOperationService = ReservedCashInOutOperationService(assetsHolder, balancesHolder, reservedCashInOutQueue)
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue, genericLimitOrderProcessorFactory)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, lkkTradesQueue, genericLimitOrderProcessorFactory)
        limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, clientsLimitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, orderBookQueue, rabbitOrderBookQueue, genericLimitOrderProcessorFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, orderBookQueue, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, rabbitOrderBookQueue, genericLimitOrderProcessorFactory)
    }

    protected fun clearMessageQueues() {
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