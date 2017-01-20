package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashTransferOperationService(private val cashOperationService: CashOperationService,
                           private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val notificationQueue: BlockingQueue<JsonSerializable>): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash transfer operation (${message.bussinesId}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")


        val operation = TransferOperation(message.fromClientId, message.toClientId, UUID.randomUUID().toString(), message.assetId,
                Date(message.dateTime), message.amount)

        val fromBalance = cashOperationService.getBalance(message.fromClientId, message.assetId)
        if (fromBalance < operation.amount) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).setRecordId(operation.uid).build())
            LOGGER.debug("Cash transfer operation (${message.bussinesId}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}: low balance for client ${message.toClientId}")
            return
        }

        processTransferOperation(operation)
        walletDatabaseAccessor.insertTransferOperation(operation)
        notificationQueue.put(operation)

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).setRecordId(operation.uid).build())
        LOGGER.debug("Cash transfer operation (${message.bussinesId}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashTransferOperation {
        return ProtocolMessages.CashTransferOperation.parseFrom(array)
    }

    fun processTransferOperation(operation: TransferOperation) {
        addToBalance(operation.fromClientId, operation.assetId, -operation.amount)
        addToBalance(operation.toClientId, operation.assetId, operation.amount)
    }

    fun addToBalance(clientId: String, assetId: String, amount: Double) {
        val balance = cashOperationService.getBalance(clientId, assetId)
        val asset = cashOperationService.getAsset(assetId)

        val newBalance = RoundingUtils.parseDouble(balance + amount, asset.accuracy).toDouble()

        cashOperationService.updateBalance(clientId, assetId, newBalance)
    }
}