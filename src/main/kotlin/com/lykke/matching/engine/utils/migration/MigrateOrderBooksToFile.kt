package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import java.util.concurrent.ConcurrentHashMap

class MigrateOrderBooksToFile {
    companion object {
        val LOGGER = Logger.getLogger(MigrateOrderBooksToFile::class.java.name)
    }

    fun migrate(config: Config) {
        teeLog("Starting migration from Azure order book to local files, path: ${config.me.orderBookPath}")

        val limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config.me.db.aLimitOrdersConnString, config.me.db.hLimitOrdersConnString, config.me.db.hLiquidityConnString)
        val fileOrderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.orderBookPath)

        val orders = limitOrderDatabaseAccessor.loadLimitOrders()
        val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()

        orders.forEach { order ->
            val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
            orderBook.addOrder(order)
        }

        limitOrdersQueues.values.forEach { book ->
            teeLog("Migrating ${book.assetId}, bids: ${book.getOrderBook(true).size} orders")
            fileOrderBookDatabaseAccessor.updateOrderBook(book.assetId, true, book.getOrderBook(true))
            teeLog("Migrating ${book.assetId}, asks: ${book.getOrderBook(false).size} orders")
            fileOrderBookDatabaseAccessor.updateOrderBook(book.assetId, false, book.getOrderBook(false))
        }

        teeLog("Migration from Azure order book to local files completed. Total order books: ${limitOrdersQueues.size * 2}. Total orders: ${orders.size}")
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}