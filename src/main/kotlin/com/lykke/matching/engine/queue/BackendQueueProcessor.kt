package com.lykke.matching.engine.queue

import com.lykke.matching.engine.daos.BtTransaction
import com.lykke.matching.engine.daos.ClientCashOperationPair
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.queue.transaction.CashIn
import com.lykke.matching.engine.queue.transaction.CashOut
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.apache.log4j.Logger
import java.util.*
import java.util.concurrent.BlockingQueue

class BackendQueueProcessor(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                            private val inQueue: BlockingQueue<Transaction>,
                            private val outQueueWriter: QueueWriter): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(BackendQueueProcessor::class.java.name)
    }

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
            LOGGER.error("Got error during message processing: ${exception.message}", exception)
        }
    }

    private fun processBitcoinCashIn(operation: CashIn) {
        LOGGER.debug("Writing CashIn operation to queue [${operation.clientId}, ${operation.Amount} ${operation.Currency}]")
        val walletCredentials = backOfficeDatabaseAccessor.loadWalletCredentials(operation.clientId)
        if (walletCredentials == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId}")
            return
        }

        val asset = backOfficeDatabaseAccessor.loadAsset(operation.Currency)
        if (asset == null) {
            LOGGER.error("No asset information for ${operation.Currency}")
            return
        }

        operation.MultisigAddress = walletCredentials.multiSig
        operation.Currency = asset.blockChainId

        val serialisedData = "CashIn:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(BtTransaction(partitionKey = "TransId", rowKey = operation.TransactionId,
                created = now, requestData = serialisedData, clientCashOperationPair = ClientCashOperationPair(operation.clientId, operation.cashOperationId)))
        outQueueWriter.write(serialisedData)
        LOGGER.debug("Wrote CashIn operation to queue [${operation.MultisigAddress}, ${operation.Amount} ${operation.Currency}]")
    }

    private fun processBitcoinCashOut(operation: CashOut) {
        LOGGER.debug("Writing CashOut operation to queue [${operation.clientId}, ${operation.Amount} ${operation.Currency}]")
        val walletCredentials = backOfficeDatabaseAccessor.loadWalletCredentials(operation.clientId)
        if (walletCredentials == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId}")
            return
        }

        val asset = backOfficeDatabaseAccessor.loadAsset(operation.Currency)
        if (asset == null) {
            LOGGER.error("No asset information for ${operation.Currency}")
            return
        }

        operation.MultisigAddress = walletCredentials.multiSig
        operation.PrivateKey = walletCredentials.privateKey
        operation.Currency = asset.blockChainId

        val serialisedData = "CashOut:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(BtTransaction(partitionKey = "TransId", rowKey = operation.TransactionId,
                created = now, requestData = serialisedData, clientCashOperationPair = ClientCashOperationPair(operation.clientId, operation.cashOperationId)))
        outQueueWriter.write(serialisedData)
        LOGGER.debug("Wrote CashOut operation to queue [${operation.MultisigAddress}, ${operation.Amount} ${operation.Currency}]")
    }

    private fun processBitcoinSwap(operation: Swap) {
        LOGGER.debug("Writing Swap operation to queue [${operation.clientId1}, ${operation.Amount1} ${operation.origAsset1} to ${operation.clientId2}, ${operation.Amount2} ${operation.origAsset2}]")
        val walletCredentials1 = backOfficeDatabaseAccessor.loadWalletCredentials(operation.clientId1)
        if (walletCredentials1 == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId1}")
            return
        }
        val walletCredentials2 = backOfficeDatabaseAccessor.loadWalletCredentials(operation.clientId2)
        if (walletCredentials2 == null) {
            LOGGER.error("No wallet credentials for client ${operation.clientId2}")
            return
        }

        val asset1 = backOfficeDatabaseAccessor.loadAsset(operation.origAsset1)
        if (asset1 == null) {
            LOGGER.error("No asset information for ${operation.origAsset1}")
            return
        }

        val asset2 = backOfficeDatabaseAccessor.loadAsset(operation.origAsset2)
        if (asset2 == null) {
            LOGGER.error("No asset information for ${operation.origAsset2}")
            return
        }

        operation.MultisigCustomer1 = walletCredentials1.multiSig
        operation.Asset1 = asset1.blockChainId
        operation.MultisigCustomer2 = walletCredentials2.multiSig
        operation.Asset2 = asset2.blockChainId

        val serialisedData = "Swap:${operation.toJson()}"

        val now = Date()
        backOfficeDatabaseAccessor.saveBitcoinTransaction(BtTransaction(partitionKey = "TransId", rowKey = operation.TransactionId,
                created = now, requestData = serialisedData, orders = operation.orders))
        outQueueWriter.write(serialisedData)
        LOGGER.debug("Wrote Swap operation to queue [${operation.MultisigCustomer1}, ${operation.Amount1} ${operation.Asset1} to ${operation.MultisigCustomer2}, ${operation.Amount2} ${operation.Asset2}]")
    }
}