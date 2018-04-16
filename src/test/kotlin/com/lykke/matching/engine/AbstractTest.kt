package com.lykke.matching.engine

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.database.TestStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.CashInOutOperationService
import com.lykke.matching.engine.services.CashTransferOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.LimitOrderMassCancelService
import com.lykke.matching.engine.services.MultiLimitOrderCancelService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.ReservedBalanceUpdateService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals

abstract class AbstractTest {

    protected val testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val stopOrderDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
    protected val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
    protected val testCashOperationsDatabaseAccessor = TestCashOperationsDatabaseAccessor()

    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    protected val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    protected val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    protected val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val clientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    protected val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val cashInOutQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val reservedCashInOutQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    protected val rabbitTransferQueue = LinkedBlockingQueue<JsonSerializable>()

    protected val assetsCache = AssetsCache(testBackOfficeDatabaseAccessor)
    protected val assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
    protected val assetsHolder = AssetsHolder(assetsCache)
    protected val assetsPairsHolder = AssetsPairsHolder(assetPairsCache)
    protected val applicationSettingsCache = ApplicationSettingsCache(testSettingsDatabaseAccessor)
    protected lateinit var balancesHolder: BalancesHolder

    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    protected lateinit var feeProcessor: FeeProcessor
    protected lateinit var genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory
    protected lateinit var limitOrdersProcessorFactory: LimitOrdersProcessorFactory

    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller
    protected lateinit var cashInOutOperationService: CashInOutOperationService
    protected lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService
    protected lateinit var cashTransferOperationsService: CashTransferOperationService
    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService
    protected lateinit var balanceUpdateService: BalanceUpdateService
    protected lateinit var reservedBalanceUpdateService: ReservedBalanceUpdateService
    protected lateinit var limitOrderCancelService: LimitOrderCancelService
    protected lateinit var limitOrderMassCancelService: LimitOrderMassCancelService
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    protected open fun initServices() {
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()

        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue, applicationSettingsCache)
        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService)

        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderService, applicationSettingsCache, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue)
        val genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService, limitOrdersProcessorFactory, clientsLimitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache)
        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(testDictionariesDatabaseAccessor, assetsPairsHolder, balancesHolder, genericLimitOrderService, genericStopLimitOrderService, genericLimitOrderProcessorFactory, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue)

        cashTransferOperationsService = CashTransferOperationService(balancesHolder, assetsHolder, applicationSettingsCache, testCashOperationsDatabaseAccessor, rabbitTransferQueue, FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService))
        minVolumeOrderCanceller = MinVolumeOrderCanceller(testDictionariesDatabaseAccessor, assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory)
        balanceUpdateService = BalanceUpdateService(balancesHolder)
        reservedBalanceUpdateService = ReservedBalanceUpdateService(balancesHolder)
        cashInOutOperationService = CashInOutOperationService(testWalletDatabaseAccessor, assetsHolder, balancesHolder, applicationSettingsCache, cashInOutQueue, feeProcessor)
        reservedCashInOutOperationService = ReservedCashInOutOperationService(assetsHolder, balancesHolder, reservedCashInOutQueue)
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrdersProcessorFactory, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue, genericLimitOrderProcessorFactory)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, lkkTradesQueue, genericLimitOrderProcessorFactory)
        limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
        limitOrderMassCancelService = LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
    }

    protected fun clearMessageQueues() {
        quotesNotificationQueue.clear()
        tradesInfoQueue.clear()
        balanceUpdateQueue.clear()
        orderBookQueue.clear()
        rabbitOrderBookQueue.clear()
        trustedClientsLimitOrdersQueue.clear()
        clientsLimitOrdersQueue.clear()
        lkkTradesQueue.clear()
        rabbitSwapQueue.clear()
        balanceNotificationQueue.clear()
    }

    protected fun assertOrderBookSize(assetPairId: String, isBuySide: Boolean, size: Int) {
        assertEquals(size, testOrderDatabaseAccessor.getOrders(assetPairId, isBuySide).size)
        assertEquals(size, genericLimitOrderService.getOrderBook(assetPairId).getOrderBook(isBuySide).size)

        // check cache orders map size
        val allClientIds = testWalletDatabaseAccessor.loadWallets().keys
        assertEquals(size, allClientIds.sumBy { genericLimitOrderService.searchOrders(it, assetPairId, isBuySide).size })
    }

    protected fun assertBalance(clientId: String, assetId: String, balance: Double? = null, reserved: Double? = null) {
        if (balance != null) {
            assertEquals(balance, balancesHolder.getBalance(clientId, assetId))
            assertEquals(balance, testWalletDatabaseAccessor.getBalance(clientId, assetId))
        }
        if (reserved != null) {
            assertEquals(reserved, balancesHolder.getReservedBalance(clientId, assetId))
            assertEquals(reserved, testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
        }
    }
}