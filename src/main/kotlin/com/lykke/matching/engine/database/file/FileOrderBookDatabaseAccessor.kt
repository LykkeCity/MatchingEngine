package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor

class FileOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir), OrderBookDatabaseAccessor {

    override fun loadLimitOrders(): List<LimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateOrderBook(asset: String, buy: Boolean, orderBook: Collection<LimitOrder>) {
         updateOrdersFile(asset , buy, orderBook)
    }

}