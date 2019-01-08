package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import redis.clients.jedis.Transaction

class AzurePersistOrdersStrategy(private val ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                 private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder) : PersistOrdersDuringRedisTransactionStrategy {
    override fun isRedisTransactionUsed(): Boolean {
        return false
    }

    override fun persist(transaction: Transaction,
                         orderBooksData: OrderBooksPersistenceData?,
                         stopOrderBooksData: OrderBooksPersistenceData?) {
        persistOrders(orderBooksData)
        persistStopOrders(stopOrderBooksData)
    }

    private fun persistOrders(data: OrderBooksPersistenceData?) {
        if (data?.isEmpty() != false) {
            return
        }

        val orderBookDatabaseAccessor = ordersDatabaseAccessorsHolder.primaryAccessor
        data.orderBooks.forEach {
            orderBookDatabaseAccessor.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
        }
    }

    private fun persistStopOrders(data: OrderBooksPersistenceData?) {
        if (data?.isEmpty() != false) {
            return
        }

        val stopOrderBookDatabaseAccessor = stopOrdersDatabaseAccessorsHolder.primaryAccessor
        data.orderBooks.forEach {
            stopOrderBookDatabaseAccessor.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
        }
    }
}