package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import java.util.concurrent.PriorityBlockingQueue

class FileOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir), OrderBookDatabaseAccessor {

    override fun loadLimitOrders(): List<LimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateOrderBook(asset: String, buy: Boolean, orderBook: PriorityBlockingQueue<LimitOrder>) {
         updateOrdersFile(asset , buy, orderBook)
    }

}