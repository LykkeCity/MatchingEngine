package com.lykke.matching.engine

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
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
import com.lykke.matching.engine.services.CashTransferOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.SingleLimitOrderService
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractTest {

    protected val testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    protected val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    protected val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    protected val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val clientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    protected val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val notificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    protected val rabbitTransferQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()

    protected val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor))
    protected val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor))
    protected lateinit var balancesHolder: BalancesHolder

    protected val trustedClients = mutableListOf<String>()

    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    protected lateinit var cashTransferOperationsService: CashTransferOperationService
    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService

    protected open fun initServices() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, notificationQueue, balanceUpdateQueue, trustedClients.toSet())
        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients.toSet())

        cashTransferOperationsService = CashTransferOperationService(balancesHolder, assetsHolder, testWalletDatabaseAccessor, rabbitTransferQueue, FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService))
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, lkkTradesQueue)
    }

}