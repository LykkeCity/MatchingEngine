package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import java.io.File
import java.io.ObjectInputStream
import java.util.LinkedList

class MigrateOrderBookFiles{
    companion object {
        val LOGGER = Logger.getLogger(MigrateOrderBookFiles::class.java.name)
    }

    fun migrate(config: Config) {
        teeLog("Starting migration from old order book format to new, path: ${config.me.orderBookPath}")

        val fileOrderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.orderBookPath)

        var count = 0

        try {
            val dir = File(config.me.orderBookPath)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
                    try {
                        val orders = loadFile(file)
                        if (orders.size > 0) {
                            val asset = orders.first().assetPairId
                            val isBuy = orders.first().isBuySide()
                            val orderBook = AssetOrderBook(asset)
                            orders.forEach {
                                val newOrder = NewLimitOrder(it.id, it.externalId, it.assetPairId, it.clientId, it.volume, it.price,
                                        it.status, it.createdAt, it.registered, it.remainingVolume, it.lastMatchTime)
                                orderBook.addOrder(newOrder)
                            }
                            fileOrderBookDatabaseAccessor.saveFile(file.name, orderBook.getOrderBook(isBuy))
                        } else {
                            LOGGER.info("Empty file: ${file.name}")
                        }
                        count++
                    } catch (e: Exception) {
                        LOGGER.error("Unable to read previous order book file ${file.name}.", e)
                    }
                }
            }
        } catch(e: Exception) {
            FileOrderBookDatabaseAccessor.LOGGER.error("Unable to load limit orders", e)
            FileOrderBookDatabaseAccessor.METRICS_LOGGER.logError(this.javaClass.name, "Unable to load limit orders", e)
        }

        teeLog("Migrated $count files")
    }


    private fun loadFile(file: File): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        if (!file.exists()) {
            throw Exception("File doesn't exist: ${file.name}")
        }
        var objectinputstream: ObjectInputStream? = null
        try {
            val streamIn = file.inputStream()
            objectinputstream = ObjectInputStream(streamIn)
            val readCase = objectinputstream.readObject() as List<*>
            readCase.forEach {
                if (it is LimitOrder) {
                    result.add(it)
                }
            }
        } finally {
            objectinputstream?.close()
        }
        return result
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}