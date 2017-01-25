package com.lykke.matching.engine.queue

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.bitcoin.BtTransaction
import com.lykke.matching.engine.daos.bitcoin.ClientCashOperationPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.logging.DATA
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_BACKEND_QUEUE
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.TYPE
import com.lykke.matching.engine.queue.transaction.CashIn
import com.lykke.matching.engine.queue.transaction.CashOut
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.HashMap
import java.util.UUID
import java.util.concurrent.BlockingQueue

class BackendQueueProcessor(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                            private val inQueue: BlockingQueue<Transaction>,
                            private val outQueueWriter: QueueWriter,
                            private val walletCredentialsCache: WalletCredentialsCache): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(BackendQueueProcessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val assets = HashMap<String, Asset>()

    private var messagesCount: Long = 0

    override fun run() {
        while (true) {
            processMessage(inQueue.take())
        }
    }

    fun processMessage(operation: Transaction) {
        try {
            when (operation) {
                is CashIn -> {
                    processBitcoinCashIn(operation)
                }
                is CashOut -> {
                    processBitcoinCashOut(operation)
                }
                is Swap -> {
                    processBitcoinSwap(operation)
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError(this.javaClass.name, "Error during message processing", exception)
        }
        METRICS_LOGGER.log(KeyValue(ME_BACKEND_QUEUE, (++messagesCount).toString()))
    }

    private fun processBitcoinCashIn(operation: CashIn) {
        LOGGER.debug("Writing CashIn operation to queue [${operation.clientId}, ${RoundingUtils.roundForPrint(operation.Amount)} ${operation.Currency}]")
        val walletCredentials = walletCredentialsCache.getWalletCredentials(operation.clientId)
        if (walletCredentials == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId}")
            METRICS_LOGGER.logError(this.javaClass.name, "No wallet credentials for client ${operation.clientId}")
            return
        }

        val asset = loadAsset(operation.Currency)
        if (asset == null) {
            LOGGER.error("No asset information for ${operation.Currency}")
            METRICS_LOGGER.logError(this.javaClass.name, "No asset information for ${operation.Currency}")
            return
        }

        operation.TransactionId = UUID.randomUUID().toString()
        operation.MultisigAddress = walletCredentials.multisig
        operation.Currency = asset.blockChainId

        val serialisedData = "CashIn:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(
                BtTransaction(operation.TransactionId!!, now, serialisedData, ClientCashOperationPair(operation.clientId, operation.cashOperationId)))
        outQueueWriter.write(serialisedData)
        LOGGER.info("Wrote CashIn operation to queue [${operation.MultisigAddress}, ${RoundingUtils.roundForPrint(operation.Amount)} ${operation.Currency}]")
        METRICS_LOGGER.log(getMetricLine("CashIn", serialisedData))
    }

    private fun processBitcoinCashOut(operation: CashOut) {
        LOGGER.debug("Writing CashOut operation to queue [${operation.clientId}, ${RoundingUtils.roundForPrint(operation.Amount)} ${operation.Currency}]")
        val walletCredentials = walletCredentialsCache.getWalletCredentials(operation.clientId)
        if (walletCredentials == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId}")
            METRICS_LOGGER.logError(this.javaClass.name, "No wallet credentials for client ${operation.clientId}")
            return
        }

        val asset = loadAsset(operation.Currency)
        if (asset == null) {
            LOGGER.error("No asset information for ${operation.Currency}")
            METRICS_LOGGER.logError(this.javaClass.name, "No asset information for ${operation.Currency}")
            return
        }

        operation.TransactionId = UUID.randomUUID().toString()
        operation.MultisigAddress = walletCredentials.multisig
        operation.Currency = asset.blockChainId

        val serialisedData = "CashOut:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(
                BtTransaction(operation.TransactionId!!, now, serialisedData, ClientCashOperationPair(operation.clientId, operation.cashOperationId)))
        outQueueWriter.write(serialisedData)
        LOGGER.info("Wrote CashOut operation to queue [${operation.MultisigAddress}, ${RoundingUtils.roundForPrint(operation.Amount)} ${operation.Currency}]")
        METRICS_LOGGER.log(getMetricLine("CashOut", serialisedData))
    }

    private fun processBitcoinSwap(operation: Swap) {
        LOGGER.debug("Writing Swap operation to queue [${operation.clientId1}, ${RoundingUtils.roundForPrint(operation.Amount1)} ${operation.origAsset1} to ${operation.clientId2}, ${RoundingUtils.roundForPrint(operation.Amount2)} ${operation.origAsset2}]")
        val walletCredentials1 = walletCredentialsCache.getWalletCredentials(operation.clientId1)
        if (walletCredentials1 == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId1}")
            METRICS_LOGGER.logError(this.javaClass.name, "No wallet credentials for client ${operation.clientId1}")
            return
        }
        val walletCredentials2 = walletCredentialsCache.getWalletCredentials(operation.clientId2)
        if (walletCredentials2 == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId2}")
            METRICS_LOGGER.logError(this.javaClass.name, "No wallet credentials for client ${operation.clientId2}")
            return
        }

        val asset1 = loadAsset(operation.origAsset1)
        if (asset1 == null) {
            LOGGER.error("No asset information for ${operation.origAsset1}")
            METRICS_LOGGER.logError(this.javaClass.name, "No asset information for ${operation.origAsset1}")
            return
        }

        val asset2 = loadAsset(operation.origAsset2)
        if (asset2 == null) {
            LOGGER.error("No asset information for ${operation.origAsset2}")
            METRICS_LOGGER.logError(this.javaClass.name, "No asset information for ${operation.origAsset2}")
            return
        }

        operation.TransactionId = UUID.randomUUID().toString()
        operation.MultisigCustomer1 = walletCredentials1.multisig
        operation.Asset1 = asset1.blockChainId
        operation.MultisigCustomer2 = walletCredentials2.multisig
        operation.Asset2 = asset2.blockChainId

        val serialisedData = "Swap:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(
                BtTransaction(operation.TransactionId!!, now, serialisedData, null, operation.orders))
        outQueueWriter.write(serialisedData)
        LOGGER.info("Wrote Swap operation to queue [${operation.MultisigCustomer1}, ${RoundingUtils.roundForPrint(operation.Amount1)} ${operation.Asset1} to ${operation.MultisigCustomer2}, ${RoundingUtils.roundForPrint(operation.Amount2)} ${operation.Asset2}]")
        METRICS_LOGGER.log(getMetricLine("Swap", serialisedData))
    }

    private fun loadAsset(assetId: String): Asset? {
        var asset = assets[assetId]
        if (asset != null) {
            return assets[assetId]
        }

        asset = backOfficeDatabaseAccessor.loadAsset(assetId)
        if (asset != null) {
            assets[assetId] = asset
        }

        return asset
    }

    fun getMetricLine(type: String, data: String): Line {
        return Line(ME_BACKEND_QUEUE, arrayOf(
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(TYPE, type),
                KeyValue(DATA, data)
        ))
    }
}