package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.INVALID_FEE
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashTransferOperationService(private val balancesHolder: BalancesHolder,
                                   private val assetsHolder: AssetsHolder,
                                   private val notificationQueue: BlockingQueue<JsonSerializable>,
                                   private val dbTransferOperationQueue: BlockingQueue<TransferOperation>,
                                   private val feeProcessor: FeeProcessor,
                                   private val cashTransferOperationValidator: CashTransferOperationValidator): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        val feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
        val feeInstructions = NewFeeInstruction.create(message.feesList)

        LOGGER.debug("Processing cash transfer operation ${message.id}) messageId: ${messageWrapper.messageId}" +
                " from client ${message.fromClientId} to client ${message.toClientId}, " +
                "asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.volume)}, " +
                "feeInstruction: $feeInstruction, feeInstructions: $feeInstructions")

        val operationId = UUID.randomUUID().toString()

        val operation = TransferOperation(operationId, message.id, message.fromClientId, message.toClientId,
                message.assetId, Date(message.timestamp),
                BigDecimal.valueOf(message.volume),
                BigDecimal.valueOf(message.overdraftLimit),
                listOfFee(feeInstruction, feeInstructions))

        try {
            cashTransferOperationValidator.performValidation(message, operationId, feeInstructions, feeInstruction)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, message, operationId, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        val fees = try {
            processTransferOperation(operation, messageWrapper.messageId!!)
        } catch (e: FeeException) {
            writeErrorResponse(messageWrapper, message, operationId, INVALID_FEE, e.message)
            return
        } catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, message, operationId, LOW_BALANCE, e.message)
            return
        } catch (e: Exception) {
            writeErrorResponse(messageWrapper, message, operationId, RUNTIME, e.message ?: "Unable to process operation")
            return
        }
        dbTransferOperationQueue.put(operation)
        notificationQueue.put(CashTransferOperation(message.id,
                operation.fromClientId,
                operation.toClientId,
                operation.dateTime,
                NumberUtils.setScaleRoundHalfUp(operation.volume, assetsHolder.getAsset(operation.asset).accuracy).toPlainString(),
                operation.overdraftLimit,
                operation.asset,
                feeInstruction,
                singleFeeTransfer(feeInstruction, fees),
                fees,
                messageWrapper.messageId!!))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operation.id)
                .setStatus(OK.type))
        LOGGER.info("Cash transfer operation (${message.id}) from client ${message.fromClientId} to client ${message.toClientId}," +
                " asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.volume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashTransferOperation {
        return ProtocolMessages.CashTransferOperation.parseFrom(array)
    }

    private fun processTransferOperation(operation: TransferOperation, messageId: String): List<Fee> {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.fromClientId, operation.asset,
                operation.dateTime, -operation.volume))
        val receiptOperation = WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.toClientId, operation.asset,
                operation.dateTime, operation.volume)
        operations.add(receiptOperation)

        val fees = feeProcessor.processFee(operation.fees, receiptOperation, operations)

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER, false)
        walletProcessor.preProcess(operations)
        val updated = walletProcessor.persistBalances()
        if (!updated) {
            throw Exception("Unable to save balance")
        }
        walletProcessor.apply().sendNotification(operation.externalId, MessageType.CASH_TRANSFER_OPERATION.name, messageId)

        return fees
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.id = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashTransferOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        return messageWrapper.parsedMessage!! as ProtocolMessages.CashTransferOperation
    }


    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   message: ProtocolMessages.CashTransferOperation,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String =  StringUtils.EMPTY) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash transfer operation (${message.id}) from client ${message.fromClientId} " +
                "to client ${message.toClientId}, asset ${message.assetId}," +
                " volume: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage")
    }
}