package com.lykke.matching.engine.outgoing.database

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

class TransferOperationSaveService(private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                                   private val transferOperations: BlockingQueue<TransferOperation>) : Thread(TransferOperationSaveService::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(TransferOperationSaveService::class.java.name)
    }

    override fun run() {
        while (true) {
            try {
                cashOperationsDatabaseAccessor.insertTransferOperation(transferOperations.take())
            } catch (e: Exception) {
                LOGGER.error("Unable to save transfer operation", e)
            }
        }
    }

}