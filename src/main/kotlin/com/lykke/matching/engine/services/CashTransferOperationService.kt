package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.Fee
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashTransferOperationService(private val balancesHolder: BalancesHolder,
                                   private val assetsHolder: AssetsHolder,
                                   private val applicationSettingsCache: ApplicationSettingsCache,
                                   private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                                   private val notificationQueue: BlockingQueue<JsonSerializable>,
                                   private val feeProcessor: FeeProcessor): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        val feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
        val feeInstructions = NewFeeInstruction.create(message.feesList)
        LOGGER.debug("""Processing cash transfer operation (${message.id}) from client ${message.fromClientId}
            | to client ${message.toClientId}, asset ${message.assetId},
            | volume: ${NumberUtils.roundForPrint(message.volume)},
            |  feeInstruction: $feeInstruction, feeInstructions: $feeInstructions""".trimMargin())

        val operationId = UUID.randomUUID().toString()

        val operation = TransferOperation(operationId, message.id, message.fromClientId, message.toClientId,
                message.assetId, Date(message.timestamp), message.volume, message.overdraftLimit, listOfFee(feeInstruction, feeInstructions))

        if (!performValidation(messageWrapper, operation, feeInstruction, feeInstructions)) {
            return
        }

        val fees = try {
            processTransferOperation(operation)
        } catch (e: FeeException) {
            writeInvalidFeeResponse(messageWrapper, message, operationId, e.message)
            return
        } catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, operationId, LOW_BALANCE, e.message)
            return
        }
        cashOperationsDatabaseAccessor.insertTransferOperation(operation)
        notificationQueue.put(CashTransferOperation(message.id, operation.fromClientId, operation.toClientId,
                operation.dateTime, operation.volume.round(assetsHolder.getAsset(operation.asset).accuracy),
                operation.overdraftLimit, operation.asset, feeInstruction, singleFeeTransfer(feeInstruction, fees), fees))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("""Cash transfer operation (${message.id}) from client ${message.fromClientId}
            |to client ${message.toClientId}, asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.volume)} processed""".trimMargin())
    }

    private fun isBalanceValid( messageWrapper: MessageWrapper, operation: TransferOperation): Boolean {
        val message = getMessage(messageWrapper)
        val fromBalance = balancesHolder.getBalance(message.fromClientId, message.assetId)
        val reservedBalance = balancesHolder.getReservedBalance(message.fromClientId, message.assetId)
        val overdraftLimit = if (operation.overdraftLimit != null) -operation.overdraftLimit else 0.0
        if (fromBalance - reservedBalance - operation.volume < overdraftLimit) {
            writeErrorResponse(messageWrapper, operation.id, LOW_BALANCE,
                    "ClientId:${message.fromClientId},asset:${message.assetId}, volume:${message.volume}")
            LOGGER.info("""Cash transfer operation (${message.id}) from client ${message.fromClientId}
                |to client ${message.toClientId}, asset ${message.assetId},
                | volume: ${NumberUtils.roundForPrint(message.volume)}: low balance for client ${message.fromClientId}""".trimMargin())
            return false
        }
        return true
    }

    private fun isAssetEnabled(messageWrapper: MessageWrapper, operationId: String): Boolean {
        val message = getMessage(messageWrapper)
        if (applicationSettingsCache.isAssetDisabled(message.assetId)) {
            writeErrorResponse(messageWrapper, operationId,  DISABLED_ASSET)

            LOGGER.info("Cash transfer operation (${message.id}) from client ${message.fromClientId} " +
                    "to client ${message.toClientId}, asset ${message.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.volume)}: disabled asset")

            return false
        }

        return true
    }

    private fun isFeeValid( messageWrapper: MessageWrapper, feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>, operationId: String): Boolean {
        val message = getMessage(messageWrapper)
        if (!checkFee(feeInstruction, feeInstructions)) {
            writeInvalidFeeResponse(messageWrapper, message, operationId)
            return false
        }

        return true
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper, transferOperationId: String): Boolean {
        val message = getMessage(messageWrapper)

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(message.volume, assetsHolder.getAsset(message.assetId).accuracy)

        if (!volumeValid) {
            writeErrorResponse(messageWrapper, transferOperationId, MessageStatus.INVALID_VOLUME_ACCURACY)
        }

        return volumeValid
    }

    private fun performValidation(messageWrapper: MessageWrapper,
                                  operation: TransferOperation,
                                  feeInstruction: FeeInstruction?,
                                  feeInstructions: List<NewFeeInstruction>): Boolean {
        val validations = arrayOf({isAssetEnabled(messageWrapper, operation.id)},
                {isFeeValid(messageWrapper, feeInstruction, feeInstructions, operation.id) },
                {isBalanceValid(messageWrapper, operation)},
                {isAccuracyValid(messageWrapper, operation.id)}
        )

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashTransferOperation {
        return ProtocolMessages.CashTransferOperation.parseFrom(array)
    }

    private fun processTransferOperation(operation: TransferOperation): List<Fee> {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.fromClientId, operation.asset,
                operation.dateTime, -operation.volume))
        val receiptOperation = WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.toClientId, operation.asset,
                operation.dateTime, operation.volume)
        operations.add(receiptOperation)

        val fees = feeProcessor.processFee(operation.fees, receiptOperation, operations)

        balancesHolder.createWalletProcessor(LOGGER, false).preProcess(operations).apply(operation.externalId, MessageType.CASH_TRANSFER_OPERATION.name)

        return fees
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

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.CashTransferOperation

    private fun writeInvalidFeeResponse(messageWrapper: MessageWrapper, message:
    ProtocolMessages.CashTransferOperation, operationId: String, errorMessage: String = "invalid fee for client") {
        writeErrorResponse(messageWrapper, operationId, INVALID_FEE, errorMessage)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String =  StringUtils.EMPTY) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage)
                .build())
        LOGGER.info("""Cash transfer operation (${message.id}) from client ${message.fromClientId}
            |to client ${message.toClientId}, asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage""".trimMargin())
        return
    }
}