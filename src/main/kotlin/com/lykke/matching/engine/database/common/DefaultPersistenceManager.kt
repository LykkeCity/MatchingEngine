package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger

class DefaultPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                private val fileProcessedMessagesDatabaseAccessor: FileProcessedMessagesDatabaseAccessor)
    : PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(DefaultPersistenceManager::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun balancesQueueSize() = 0

    override fun persist(data: PersistenceData): Boolean {
        return try {
            persistData(data)
            persistProcessedMessages(data.message)
            true
        } catch (e: Exception) {
            val retryMessage = "Unable to save data (${data.details()}), retrying"
            LOGGER.error(retryMessage, e)
            METRICS_LOGGER.logError(retryMessage, e)

            return try {
                persistData(data)
                true
            } catch (e: Exception) {
                val message = "Unable to save data (${data.details()})"
                LOGGER.error(message, e)
                METRICS_LOGGER.logError(message, e)
                false
            }
        }
    }

    private fun persistData(data: PersistenceData) {
        walletDatabaseAccessor.insertOrUpdateWallets(data.wallets.toList())
    }

    private fun persistProcessedMessages(processedMessage: ProcessedMessage?) {
        if (processedMessage != null) {
            fileProcessedMessagesDatabaseAccessor.saveProcessedMessage(processedMessage)
        }
    }
}