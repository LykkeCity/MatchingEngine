package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.ArrayList
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue


class FileOrderBookDatabaseAccessor(private val ordersDir: String, private val stopOrdersDir: String): OrderBookDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(FileOrderBookDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var conf = FSTConfiguration.createDefaultConfiguration()

    init {
        val dir = File(stopOrdersDir)
        if (!dir.exists()) {
            dir.mkdirs()
        }
    }

    override fun loadLimitOrders(): List<NewLimitOrder> = loadLimitOrders(ordersDir)

    private fun loadLimitOrders(ordersDir: String): List<NewLimitOrder> {
        val result = ArrayList<NewLimitOrder>()
        try {
            val dir = File(ordersDir)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
                    if (!file.isDirectory && !file.name.startsWith("_prev_")) {
                        try {
                            result.addAll(loadFile(file))
                        } catch (e: Exception) {
                            LOGGER.error("Unable to read order book file ${file.name}. Trying to load previous one", e)
                            try {
                                result.addAll(loadFile(File("$ordersDir/_prev_${file.name}")))
                            } catch (e: Exception) {
                                LOGGER.error("Unable to read previous order book file ${file.name}.", e)
                            }
                        }
                    }
                }
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load limit orders", e)
            METRICS_LOGGER.logError( "Unable to load limit orders", e)
        }
        LOGGER.info("Loaded ${result.size} active limit orders")
        return result
    }

    private fun loadFile(file: File): List<NewLimitOrder> {
        val result = LinkedList<NewLimitOrder>()
        val fileLocation = file.toPath()
        val bytes = Files.readAllBytes(fileLocation)
        val readCase = conf.asObject(bytes)
        if (readCase is List<*>) {
            readCase.forEach {
                if (it is NewLimitOrder) {
                    result.add(it)
                }
            }
        }
        return result
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<NewLimitOrder>) {
        try {
            val fileName = "${asset}_$isBuy"
            archiveAndDeleteFile(ordersDir, fileName)
            saveFile(ordersDir, fileName, orderBook.toList())
        } catch(e: Exception) {
            LOGGER.error("Unable to save order book, size: ${orderBook.size}", e)
            METRICS_LOGGER.logError( "Unable to save order book, size: ${orderBook.size}", e)
        }
    }

    private fun saveFile(ordersDir: String, fileName: String, orderBook: List<NewLimitOrder>) {
        try {
            val file = File("$ordersDir/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asByteArray(orderBook)
            Files.write(FileSystems.getDefault().getPath("$ordersDir/$fileName"), bytes, StandardOpenOption.CREATE)
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }

    private fun archiveAndDeleteFile(ordersDir: String, fileName: String) {
        try {
            val newFile = FileSystems.getDefault().getPath("$ordersDir/_prev_$fileName")
            val oldFile = FileSystems.getDefault().getPath("$ordersDir/$fileName")
            Files.move(oldFile, newFile, StandardCopyOption.REPLACE_EXISTING)
        } catch(e: Exception) {
            LOGGER.error("Unable to archive and delete, name: $fileName", e)
            METRICS_LOGGER.logError( "Unable to archive and delete, name: $fileName", e)
        }
    }

    override fun loadStopLimitOrders(): List<NewLimitOrder> {
        LOGGER.info("Start stop limit orders loading")
        val result = loadLimitOrders(stopOrdersDir)
        LOGGER.info("Loaded stop limit orders")
        return result
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        try {
            val fileName = "${assetPairId}_$isBuy"
            archiveAndDeleteFile(stopOrdersDir, fileName)
            saveFile(stopOrdersDir, fileName, orderBook.toList())
        } catch(e: Exception) {
            LOGGER.error("Unable to save stop order book, size: ${orderBook.size}", e)
            METRICS_LOGGER.logError( "Unable to save stop order book, size: ${orderBook.size}", e)
        }
    }
}