package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.LinkedList

open class AbstractFileOrderBookDatabaseAccessor(private val ordersDir: String,
                                                 logPrefix: String = "") {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AbstractFileOrderBookDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val logPrefix = if (logPrefix.isNotEmpty()) "$logPrefix " else ""
    private var conf = FSTConfiguration.createDefaultConfiguration()

    init {
        val dir = File(ordersDir)
        if (!dir.exists()) {
            dir.mkdirs()
        }
    }

    fun loadOrdersFromFiles(): List<NewLimitOrder> {
        val result = ArrayList<NewLimitOrder>()
        try {
            val dir = File(ordersDir)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
                    if (!file.isDirectory && !file.name.startsWith("_prev_")) {
                        try {
                            result.addAll(loadFile(file))
                        } catch (e: Exception) {
                            LOGGER.error("Unable to read ${logPrefix}order book file ${file.name}. Trying to load previous one", e)
                            try {
                                result.addAll(loadFile(File("$dir/_prev_${file.name}")))
                            } catch (e: Exception) {
                                LOGGER.error("Unable to read previous ${logPrefix}order book file ${file.name}.", e)
                            }
                        }
                    }
                }
            }
        } catch(e: Exception) {
            val message = "Unable to load ${logPrefix}limit orders"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
        }
        LOGGER.info("Loaded ${result.size} active ${logPrefix}limit orders")
        return result
    }

    protected fun updateOrdersFile(fileName: String, orders: Collection<NewLimitOrder>) {
        try {
            archiveAndDeleteFile(fileName)
            saveFile(fileName, orders.toList())
        } catch(e: Exception) {
            val message = "Unable to save ${logPrefix}order book, size: ${orders.size}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
        }
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

    private fun saveFile(fileName: String, data: List<NewLimitOrder>) {
        try {
            val file = File("$ordersDir/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asByteArray(data)
            Files.write(FileSystems.getDefault().getPath("$ordersDir/$fileName"), bytes, StandardOpenOption.CREATE)
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }

    private fun archiveAndDeleteFile(fileName: String) {
        try {
            val newFile = FileSystems.getDefault().getPath("$ordersDir/_prev_$fileName")
            val oldFile = FileSystems.getDefault().getPath("$ordersDir/$fileName")
            Files.move(oldFile, newFile, StandardCopyOption.REPLACE_EXISTING)
        } catch(e: Exception) {
            val message = "Unable to archive and delete, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
        }
    }
}