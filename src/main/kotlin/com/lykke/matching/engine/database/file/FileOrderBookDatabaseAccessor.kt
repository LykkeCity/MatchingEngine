package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor

class FileOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir), OrderBookDatabaseAccessor {

    override fun loadLimitOrders(): List<NewLimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        val fileName = "${asset}_$isBuy"
        updateOrdersFile(fileName, orderBook)
    }

}