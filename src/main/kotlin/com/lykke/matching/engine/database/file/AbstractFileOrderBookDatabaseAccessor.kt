package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import java.math.BigDecimal
import java.nio.file.*
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

open class AbstractFileOrderBookDatabaseAccessor(private val ordersDir: String,
                                                 logPrefix: String = "") {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AbstractFileOrderBookDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val PREV_ORDER_BOOK_FILE_PEFIX = "_prev_"
    }

    private val logPrefix = if (logPrefix.isNotEmpty()) "$logPrefix " else ""
    private var conf = FSTConfiguration.createDefaultConfiguration()

    init {
        Files.createDirectories( Paths.get(ordersDir))
    }

    fun loadOrdersFromFiles(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()

        try {
            val orderDirPath = Paths.get(ordersDir)
            if (Files.notExists(orderDirPath)) {
                return result
            }

            Files.list(orderDirPath)
                    .filter { path -> !Files.isDirectory(path) && !path.fileName.startsWith(PREV_ORDER_BOOK_FILE_PEFIX) }
                    .map { readOrderBookFileOrPrevFileOnFail(it) }
                    .collect(Collectors.toList())


        } catch(e: Exception) {
            val message = "Unable to load ${logPrefix}limit orders"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
        }

        LOGGER.info("Loaded ${result.size} active ${logPrefix}limit orders")
        return result
    }

    private fun readOrderBookFileOrPrevFileOnFail(filePath: Path): List<LimitOrder> {
        try {
            return readFile(filePath)
        } catch (e: Exception) {
            LOGGER.error("Unable to read ${logPrefix}order book file ${filePath.fileName}. Trying to load previous one", e)
            readPrevOrderBookFile(filePath.fileName.toString())
        }

        return Collections.emptyList()
    }

    private fun readPrevOrderBookFile(fileName: String): List<LimitOrder> {
        try {
            return readFile(getPrevOrderBookFilePath(fileName))
        } catch (e: Exception) {
            LOGGER.error("Unable to read previous ${logPrefix}order book file $fileName.", e)
        }

        return Collections.emptyList()
    }

    private fun getPrevOrderBookFilePath(fileName: String): Path {
        return Paths.get(ordersDir, "$PREV_ORDER_BOOK_FILE_PEFIX$fileName")
    }

    private fun getOrderBookFilePath(fileName: String): Path {
        return Paths.get(ordersDir, fileName)
    }

    private fun getOrderBookFileName(asset: String, buy: Boolean): String {
        return "${asset}_$buy"
    }

    protected fun updateOrdersFile(asset: String, buy: Boolean,  orders: Collection<LimitOrder>) {
        try {
            val fileName = getOrderBookFileName(asset, buy)
            archiveAndDeleteFile(fileName)
            saveFile(fileName, orders.toList())
        } catch(e: Exception) {
            val message = "Unable to save ${logPrefix}order book, size: ${orders.size}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
        }
    }

    private fun readFile(filePath: Path): List<LimitOrder> {
        val bytes = Files.readAllBytes(filePath)
        val orders = conf.asObject(bytes)

        if (orders is List<*>) {
            return Stream.concat(readOldOrderFormat(orders).stream(),
                    readOrders(orders).stream())
                    .collect(Collectors.toCollection { LinkedList<LimitOrder>() })
        }

        return LinkedList()
    }

    private fun readOrders(orders: List<*>): List<LimitOrder> {
        return orders
                .stream()
                .filter { it is  LimitOrder}
                .map { it as LimitOrder}
                .collect(Collectors.toCollection({ LinkedList<LimitOrder>() }))
    }

    private fun readOldOrderFormat(orders: List<*>): List<LimitOrder> {
        return orders
                .stream()
                .filter { it is  NewLimitOrder}
                .map { fromNewLimitOrderToLimitOrder(it as NewLimitOrder)}
                .collect(Collectors.toCollection({ LinkedList<LimitOrder>() }))
    }

    private fun fromNewLimitOrderToLimitOrder(order: NewLimitOrder): LimitOrder {
        return LimitOrder(
                order.id,
                order.externalId,
                order.assetPairId,
                order.clientId,
                BigDecimal.valueOf(order.volume),
                BigDecimal.valueOf(order.price),
                order.status,
                order.createdAt,
                order.registered,
                BigDecimal.valueOf(order.remainingVolume),
                order.lastMatchTime,
                order.reservedLimitVolume?.toBigDecimal(),
                order.fee as LimitOrderFeeInstruction?,
                order.fees as List<NewLimitOrderFeeInstruction>?,
                order.type,
                order.lowerLimitPrice?.toBigDecimal(),
                order.lowerPrice?.toBigDecimal(),
                order.upperLimitPrice?.toBigDecimal(),
                order.upperPrice?.toBigDecimal(),
                order.previousExternalId
        )
    }

    private fun saveFile(fileName: String, data: List<LimitOrder>) {
        try {
            val bytes = conf.asByteArray(data)
            Files.write(getOrderBookFilePath(fileName), bytes, StandardOpenOption.CREATE)
        } catch (e: Exception) {
            val message = "Unable to save order book file, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
            throw e
        }
    }

    private fun archiveAndDeleteFile(fileName: String) {
        try {
            val prevOrderBookFile = getPrevOrderBookFilePath(fileName)
            val orderBookFile = getOrderBookFilePath(fileName)
            Files.move(orderBookFile, prevOrderBookFile, StandardCopyOption.REPLACE_EXISTING)
        } catch(e: Exception) {
            val message = "Unable to archive and delete, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError( message, e)
            throw e
        }
    }
}