package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import org.apache.log4j.Logger
import java.io.File
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.ArrayList
import java.util.concurrent.PriorityBlockingQueue

class FileOrderBookDatabaseAccessor(private val ordersDir: String): OrderBookDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(FileOrderBookDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun loadLimitOrders(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()
        try {
            val dir = File(ordersDir)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
                    if (!file.name.startsWith("prev_")) {
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
                        } catch (e: Exception) {
                            e.printStackTrace()
                        } finally {
                            objectinputstream?.close()
                        }
                    }
                }
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load limit orders", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load limit orders", e)
        }
        LOGGER.info("Loaded ${result.size} active limit orders")
        return result
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<LimitOrder>) {
        try {
            val fileName = "$ordersDir/${asset}_$isBuy"
            archiveAndDeleteFile(fileName)
            var oos: ObjectOutputStream? = null
            try {
                val file = File(fileName)
                if (!file.exists()) {
                    file.createNewFile()
                }
                oos = ObjectOutputStream(file.outputStream())
                oos.writeObject(orderBook.toList())
            } catch (ex: Exception) {
                ex.printStackTrace()
            } finally {
                oos?.close()
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to save order book, size: ${orderBook.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to save order book, size: ${orderBook.size}", e)
        }
    }

    private fun archiveAndDeleteFile(fileName: String) {
        try {
            val newFileName = "$ordersDir/_prev_$fileName"
            val oldFileName = "$ordersDir/$fileName"
            val newFile = File(newFileName)
            if (newFile.exists()){
                newFile.delete()
            }
            val oldFile = File(oldFileName)
            if (oldFile.exists()) {
                oldFile.renameTo(File(newFileName))
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to archive and delete, name: $fileName", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to archive and delete, name: $fileName", e)
        }
    }
}