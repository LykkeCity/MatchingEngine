package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashTransferOperationService( private val balancesHolder: BalancesHolder,
                                    private val assetsHolder: AssetsHolder,
                                    private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                    private val notificationQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.CashOperation> {

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    private val feeProcessor = FeeProcessor(balancesHolder, assetsHolder)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashTransferOperation
        val fee = FeeInstruction.create(message.fee)
        LOGGER.debug("Processing cash transfer operation (${message.id}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, fee: $fee")

        val operation = TransferOperation(UUID.randomUUID().toString(), message.id, message.fromClientId, message.toClientId, message.assetId, Date(message.timestamp), message.volume, message.overdraftLimit, fee)

        val fromBalance = balancesHolder.getBalance(message.fromClientId, message.assetId)
        val reservedBalance = balancesHolder.getReservedBalance(message.fromClientId, message.assetId)
        val overdraftLimit = if (operation.overdraftLimit != null) -operation.overdraftLimit else 0.0
        if (fromBalance - reservedBalance - operation.volume < overdraftLimit) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                    .setStatus(LOW_BALANCE.type).setStatusReason("ClientId:${message.fromClientId},asset:${message.assetId}, volume:${message.volume}").build())
            LOGGER.info("Cash transfer operation (${message.id}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)}: low balance for client ${message.fromClientId}")
            return
        }

        val feeTransfer = processTransferOperation(operation)
        walletDatabaseAccessor.insertTransferOperation(operation)
        notificationQueue.put(CashTransferOperation(message.id, operation.fromClientId, operation.toClientId, operation.dateTime, operation.volume.round(assetsHolder.getAsset(operation.asset).accuracy), operation.overdraftLimit, operation.asset, operation.fee, feeTransfer))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash transfer operation (${message.id}) from client ${message.fromClientId} to client ${message.toClientId}, asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashTransferOperation {
        return ProtocolMessages.CashTransferOperation.parseFrom(array)
    }

    private fun processTransferOperation(operation: TransferOperation): FeeTransfer? {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.fromClientId, operation.asset,
                operation.dateTime, -operation.volume))
        val receiptOperation = WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.toClientId, operation.asset,
                operation.dateTime, operation.volume)
        operations.add(receiptOperation)

        val feeTransfer = feeProcessor.processFee(operation.fee, receiptOperation, operations)

        balancesHolder.processWalletOperations(operation.externalId, MessageType.CASH_TRANSFER_OPERATION.name, operations)

        return feeTransfer
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashTransferOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }
}