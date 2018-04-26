package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashSwapOperationService(private val balancesHolder: BalancesHolder,
                               private val assetsHolder: AssetsHolder,
                               private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                               private val notificationQueue: BlockingQueue<JsonSerializable>) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("""Processing cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${NumberUtils.roundForPrint(message.volume1)}
            |to client ${message.clientId2}, asset ${message.assetId2}, amount: ${NumberUtils.roundForPrint(message.volume2)}""".trimMargin())

        val operation = SwapOperation(UUID.randomUUID().toString(), message.id, Date(message.timestamp),
                message.clientId1, message.assetId1, message.volume1,
                message.clientId2, message.assetId2, message.volume2)

        if (!performValidation(messageWrapper, operation)) {
            return
        }

        try {
            processSwapOperation(operation)
        } catch (e: BalanceException) {
            LOGGER.info("Cash swap operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, operation, MessageStatus.LOW_BALANCE, e.message)
            return
        }
        cashOperationsDatabaseAccessor.insertSwapOperation(operation)
        notificationQueue.put(CashSwapOperation(operation.externalId, operation.dateTime,
                operation.clientId1, operation.asset1, operation.volume1.round(assetsHolder.getAsset(operation.asset1).accuracy),
                operation.clientId2, operation.asset2, operation.volume2.round(assetsHolder.getAsset(operation.asset2).accuracy)))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("""Cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1},
            |amount: ${NumberUtils.roundForPrint(message.volume1)} to client ${message.clientId2}, asset ${message.assetId2},
            |amount: ${NumberUtils.roundForPrint(message.volume2)} processed""".trimMargin())
    }

    private fun isBalanceValid(client: String, assetId: String,
                               volume: Double, operation: SwapOperation,
                               messageWrapper: MessageWrapper): Boolean {
        val balance = balancesHolder.getBalance(client, assetId)
        val reservedBalance = balancesHolder.getReservedBalance(client, assetId)
        if (balance - reservedBalance < operation.volume1) {
            writeErrorResponse(messageWrapper, operation, LOW_BALANCE,"ClientId:$client,asset:$assetId, volume:$volume")
            LOGGER.info("Cash swap operation failed due to low balance: $client, $volume $assetId")
            return false
        }

        return true
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper, assetId: String, volume: Double, operation: SwapOperation): Boolean {
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(volume, assetsHolder.getAsset(assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid, assetId: $assetId, clientId1 ${operation.clientId1}, clientId2 ${operation.clientId2}, volume $volume")
            writeErrorResponse(messageWrapper, operation, MessageStatus.INVALID_VOLUME_ACCURACY)
        }

        return volumeValid
    }

    private fun performValidation(messageWrapper: MessageWrapper, operation: SwapOperation): Boolean {
        val message = getMessage(messageWrapper)
        val validations = arrayOf({ isBalanceValid(message.clientId1, message.assetId1, operation.volume1, operation, messageWrapper) },
                { isBalanceValid(message.clientId2, message.assetId2, operation.volume2, operation, messageWrapper) },
                { isAccuracyValid(messageWrapper, message.assetId1, operation.volume1, operation) },
                { isAccuracyValid(messageWrapper, message.assetId2, operation.volume2, operation) })

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashSwapOperation {
        return ProtocolMessages.CashSwapOperation.parseFrom(array)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper, operation: SwapOperation, status: MessageStatus, errorMessage: String = StringUtils.EMPTY) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operation.id)
                .setStatus(status.type).setStatusReason(errorMessage).build())
    }

    private fun processSwapOperation(operation: SwapOperation) {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset1,
                operation.dateTime, -operation.volume1))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset1,
                operation.dateTime, operation.volume1))

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset2,
                operation.dateTime, operation.volume2))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset2,
                operation.dateTime, -operation.volume2))

        balancesHolder.createWalletProcessor(LOGGER).preProcess(operations).apply(operation.externalId, MessageType.CASH_SWAP_OPERATION.name)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashSwapOperation {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashSwapOperation
        return message
    }
}