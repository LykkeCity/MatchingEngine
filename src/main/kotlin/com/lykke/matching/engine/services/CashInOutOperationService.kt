package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.Fee
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.INVALID_FEE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val assetsHolder: AssetsHolder,
                                private val balancesHolder: BalancesHolder,
                                private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>,
                                private val feeProcessor: FeeProcessor,
                                private val cashInOutOperationValidator: CashInOutOperationValidator) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)

        val feeInstructions = NewFeeInstruction.create(message.feesList)
        LOGGER.debug("Processing cash in/out messageId: ${messageWrapper.messageId} operation (${message.id})" +
                " for client ${message.clientId}, asset ${message.assetId}," +
                " amount: ${NumberUtils.roundForPrint(message.volume)}, feeInstructions: $feeInstructions")

        val operationId = UUID.randomUUID().toString()

        val walletOperation = getWalletOperation(operationId, message)
        val operations = mutableListOf(walletOperation)

        try {
            cashInOutOperationValidator.performValidation(message, feeInstructions)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        val fees = try {
            feeProcessor.processFee(feeInstructions, walletOperation, operations)
        } catch (e: FeeException) {
            writeErrorResponse(messageWrapper, walletOperation.id, INVALID_FEE, e.message)
            return
        }

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(operations)
        }  catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        val updated = walletProcessor.persistBalances(ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!), null, null)
        messageWrapper.processedMessagePersisted = true
        if (!updated) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(walletOperation.id)
                    .setStatus(MessageStatus.RUNTIME.type))
            LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.volume)}: unable to save balance")
            return
        }
        walletProcessor.apply().sendNotification(message.id, MessageType.CASH_IN_OUT_OPERATION.name, messageWrapper.messageId!!)

        publishRabbitMessage(message, walletOperation, fees, messageWrapper.messageId!!)

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(walletOperation.id)
                .setStatus(OK.type))

        LOGGER.info("Cash in/out walletOperation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume)} processed")
    }

    private fun publishRabbitMessage(message: ProtocolMessages.CashInOutOperation,
                                     walletOperation: WalletOperation,
                                     fees: List<Fee>,
                                     messageId: String) {
        rabbitCashInOutQueue.put(CashOperation(
                message.id,
                walletOperation.clientId,
                walletOperation.dateTime,
                walletOperation.amount.round(assetsHolder.getAsset(walletOperation.assetId).accuracy),
                walletOperation.assetId,
                messageId,
                fees
        ))
    }

    private fun getWalletOperation(operationId: String, message: ProtocolMessages.CashInOutOperation): WalletOperation {
        return WalletOperation(operationId, message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)
    }
    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashInOutOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        return messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashInOutOperation {
        return ProtocolMessages.CashInOutOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.id
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage")
    }
}