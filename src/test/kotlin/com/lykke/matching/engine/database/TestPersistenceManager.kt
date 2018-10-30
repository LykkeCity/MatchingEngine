package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder

class TestPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                             private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                             private val stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor): PersistenceManager {

    var persistenceErrorMode = false

    override fun persist(data: PersistenceData): Boolean {
        if (persistenceErrorMode) {
            return false
        }
        if (data.balancesData?.wallets?.isNotEmpty() == true) {
            walletDatabaseAccessor.insertOrUpdateWallets(ArrayList(data.balancesData?.wallets))
        }
        data.orderBooksData?.let {orderBooksPersistenceData ->
            (orderBookDatabaseAccessorHolder.primaryAccessor as TestOrderBookDatabaseAccessor).updateOrders(orderBooksPersistenceData.ordersToSave,
                    orderBooksPersistenceData.ordersToRemove)

            orderBooksPersistenceData.orderBooks.forEach {
                orderBookDatabaseAccessorHolder.secondaryAccessor!!.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
            }
        }
        data.stopOrderBooksData?.orderBooks?.forEach {
            stopOrderBookDatabaseAccessor.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
        }
        return true
    }
}