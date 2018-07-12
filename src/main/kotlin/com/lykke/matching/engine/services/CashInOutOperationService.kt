package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.INVALID_FEE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val assetsHolder: AssetsHolder,
                                private val balancesHolder: BalancesHolder,
                                private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>,
                                private val feeProcessor: FeeProcessor,
                                private val cashInOutOperationValidator: CashInOutOperationValidator,
                                private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                private val messageSender: MessageSender) : AbstractService {
    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val cashInOutContext: CashInOutContext = messageWrapper.context as CashInOutContext
        val feeInstructions = cashInOutContext.feeInstructions
        val walletOperation = cashInOutContext.walletOperation

        LOGGER.debug("Processing cash in/out messageId: ${cashInOutContext.messageId} operation (${cashInOutContext.id})" +
                " for client ${cashInOutContext.clientId}, asset ${cashInOutContext.asset.assetId}," +
                " amount: ${NumberUtils.roundForPrint(walletOperation.amount)}, feeInstructions: $feeInstructions")

        val operations = mutableListOf(walletOperation)

        try {
            cashInOutOperationValidator.performValidation(cashInOutContext)
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
        } catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(cashInOutContext.processedMessage, sequenceNumber)
        messageWrapper.processedMessagePersisted = true
        if (!updated) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(walletOperation.id)
                    .setStatus(MessageStatus.RUNTIME.type))
            LOGGER.info("Cash in/out operation (${cashInOutContext.id}) for client ${cashInOutContext.clientId} asset ${cashInOutContext.asset.assetId}, volume: ${NumberUtils.roundForPrint(walletOperation.amount)}: unable to save balance")
            return
        }
        walletProcessor.apply().sendNotification(cashInOutContext.id, MessageType.CASH_IN_OUT_OPERATION.name, messageWrapper.messageId!!)

        publishRabbitMessage(cashInOutContext, fees)

        val outgoingMessage = EventFactory.createCashInOutEvent(walletOperation.amount,
                sequenceNumber,
                cashInOutContext.messageId,
                cashInOutContext.id,
                cashInOutContext.operationStartTime,
                MessageType.CASH_IN_OUT_OPERATION,
                walletProcessor.getClientBalanceUpdates(),
                walletOperation,
                fees)

        messageSender.sendMessage(outgoingMessage)

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(walletOperation.id)
                .setStatus(OK.type))

        LOGGER.info("Cash in/out walletOperation (${cashInOutContext.id}) for client ${cashInOutContext.clientId}, " +
                "asset ${cashInOutContext.asset.assetId}, " +
                "amount: ${NumberUtils.roundForPrint(walletOperation.amount)} processed")
    }

    private fun publishRabbitMessage(cashInOutContext: CashInOutContext,
                                     fees: List<Fee>) {
        val walletOperation = cashInOutContext.walletOperation
        rabbitCashInOutQueue.put(CashOperation(
                cashInOutContext.id,
                walletOperation.clientId,
                walletOperation.dateTime,
                NumberUtils.setScaleRoundHalfUp(walletOperation.amount, assetsHolder.getAsset(walletOperation.assetId).accuracy).toPlainString(),
                cashInOutContext.asset.assetId,
                cashInOutContext.messageId,
                fees
        ))
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        val context = messageWrapper.context as CashInOutContext
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash in/out operation (${context.id}) for client ${context.clientId}, " +
                "asset ${context.asset.assetId}, amount: ${NumberUtils.roundForPrint(context.walletOperation.amount)}: $errorMessage")
    }
}