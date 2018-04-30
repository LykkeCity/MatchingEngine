package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor

class FileStopOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir, "stop"), StopOrderBookDatabaseAccessor {

    override fun loadStopLimitOrders(): List<NewLimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        val fileName = "${assetPairId}_$isBuy"
        updateOrdersFile(fileName, orderBook)
    }

}