package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import java.util.concurrent.PriorityBlockingQueue

class FileOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir), OrderBookDatabaseAccessor {

    override fun loadLimitOrders(): List<NewLimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateOrderBook(asset: String, buy: Boolean, orderBook: PriorityBlockingQueue<NewLimitOrder>) {
         updateOrdersFile(asset , buy, orderBook)
    }

}