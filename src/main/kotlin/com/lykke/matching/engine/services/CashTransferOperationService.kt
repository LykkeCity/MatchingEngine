package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
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
import com.lykke.matching.engine.outgoing.messages.v2.events.CashTransferEvent
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashTransferOperationService(private val balancesHolder: BalancesHolder,
                                   private val notificationQueue: BlockingQueue<JsonSerializable>,
                                   private val dbTransferOperationQueue: BlockingQueue<TransferOperation>,
                                   private val feeProcessor: FeeProcessor,
                                   private val cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator,
                                   private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                   private val messageSender: MessageSender) : AbstractService {
    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val cashTransferContext = messageWrapper.context as CashTransferContext

        val transferOperation = cashTransferContext.transferOperation

        LOGGER.debug("Processing cash transfer operation ${transferOperation.externalId}) messageId: ${cashTransferContext.messageId}" +
                " from client ${transferOperation.fromClientId} to client ${transferOperation.toClientId}, " +
                "asset ${transferOperation.asset}, volume: ${NumberUtils.roundForPrint(transferOperation.volume)}, " +
                "feeInstructions: ${transferOperation.fees}")

        try {
            cashTransferOperationBusinessValidator.performValidation(cashTransferContext)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, cashTransferContext, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        val result = try {
            messageWrapper.processedMessagePersisted = true
            processTransferOperation(transferOperation, cashTransferContext.messageId, cashTransferContext.processedMessage, cashTransferContext.operationStartTime)
        } catch (e: FeeException) {
            writeErrorResponse(messageWrapper, cashTransferContext, INVALID_FEE, e.message)
            return
        } catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, cashTransferContext, LOW_BALANCE, e.message)
            return
        } catch (e: Exception) {
            writeErrorResponse(messageWrapper, cashTransferContext, RUNTIME, e.message ?: "Unable to process operation")
            return
        }
        dbTransferOperationQueue.put(transferOperation)
        val fee = if(transferOperation.fees == null || transferOperation.fees.isEmpty()) null else transferOperation.fees.first()

        notificationQueue.put(CashTransferOperation(transferOperation.externalId,
                transferOperation.fromClientId,
                transferOperation.toClientId,
                transferOperation.dateTime,
                NumberUtils.setScaleRoundHalfUp(transferOperation.volume, cashTransferContext.asset!!.accuracy).toPlainString(),
                transferOperation.overdraftLimit,
                transferOperation.asset,
                fee,
                singleFeeTransfer(fee, result.fees),
                result.fees,
                cashTransferContext.messageId))

        messageSender.sendMessage(result.outgoingMessage)

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(transferOperation.id)
                .setStatus(OK.type))
        LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} to client ${transferOperation.toClientId}," +
                " asset ${transferOperation.asset}, volume: ${NumberUtils.roundForPrint(transferOperation.volume)} processed")
    }

    private fun processTransferOperation(operation: TransferOperation,
                                         messageId: String,
                                         processedMessage: ProcessedMessage?,
                                         date: Date): OperationResult {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.fromClientId, operation.asset,
                operation.dateTime, -operation.volume))
        val receiptOperation = WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.toClientId, operation.asset,
                operation.dateTime, operation.volume)
        operations.add(receiptOperation)

        val fees = feeProcessor.processFee(operation.fees, receiptOperation, operations)

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER, false)
        walletProcessor.preProcess(operations)

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(processedMessage, null, null, sequenceNumber)
        if (!updated) {
            throw Exception("Unable to save balance")
        }
        walletProcessor.apply().sendNotification(operation.externalId, MessageType.CASH_TRANSFER_OPERATION.name, messageId)

        val outgoingMessage = EventFactory.createCashTransferEvent(sequenceNumber,
                messageId,
                operation.externalId,
                date,
                MessageType.CASH_TRANSFER_OPERATION,
                walletProcessor.getClientBalanceUpdates(),
                operation,
                fees)

        return OperationResult(outgoingMessage, fees)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   context: CashTransferContext,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(context.transferOperation.id)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash transfer operation (${context.transferOperation.externalId}) from client ${context.transferOperation.fromClientId} " +
                "to client ${context.transferOperation.toClientId}, asset ${context.asset}," +
                " volume: ${NumberUtils.roundForPrint(context.transferOperation.volume)}: $errorMessage")
    }
}

private class OperationResult(val outgoingMessage: CashTransferEvent,
                              val fees: List<Fee>)