package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor

class FileStopOrderBookDatabaseAccessor(ordersDir: String) : AbstractFileOrderBookDatabaseAccessor(ordersDir, "stop"), StopOrderBookDatabaseAccessor {

    override fun loadStopLimitOrders(): List<LimitOrder> {
        return loadOrdersFromFiles()
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        updateOrdersFile(assetPairId, isBuy, orderBook)
    }
}