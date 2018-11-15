package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder

class TestPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                             private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                             private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder) : PersistenceManager {

    var persistenceErrorMode = false

    override fun persist(data: PersistenceData): Boolean {
        if (persistenceErrorMode) {
            return false
        }
        if (data.balancesData?.wallets?.isNotEmpty() == true) {
            walletDatabaseAccessor.insertOrUpdateWallets(ArrayList(data.balancesData?.wallets))
        }
        data.orderBooksData?.let { orderBooksPersistenceData ->
            (orderBookDatabaseAccessorHolder.primaryAccessor as TestOrderBookDatabaseAccessor).updateOrders(orderBooksPersistenceData.ordersToSave,
                    orderBooksPersistenceData.ordersToRemove)

            orderBooksPersistenceData.orderBooks.forEach {
                orderBookDatabaseAccessorHolder.secondaryAccessor!!.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
            }
        }
        data.stopOrderBooksData?.let { stopOrderBooksPersistenceData ->
            (stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor).updateOrders(stopOrderBooksPersistenceData.ordersToSave,
                    stopOrderBooksPersistenceData.ordersToRemove)

            stopOrderBooksPersistenceData.orderBooks.forEach {
                stopOrdersDatabaseAccessorsHolder.secondaryAccessor!!.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
            }
        }
        return true
    }
}