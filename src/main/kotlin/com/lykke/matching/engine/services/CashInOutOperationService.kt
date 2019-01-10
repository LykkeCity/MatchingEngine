package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.converters.CashInOutOperationConverter
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.INVALID_FEE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.BlockingQueue

@Service
class CashInOutOperationService(private val balancesHolder: BalancesHolder,
                                private val rabbitCashInOutQueue: BlockingQueue<CashOperation>,
                                private val feeProcessor: FeeProcessor,
                                private val cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator,
                                private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                private val messageSender: MessageSender) : AbstractService {
    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val cashInOutContext: CashInOutContext = messageWrapper.context as CashInOutContext
        val cashInOutOperation = cashInOutContext.cashInOutOperation
        val feeInstructions = cashInOutOperation.feeInstructions
        val walletOperation = CashInOutOperationConverter.fromCashInOutOperationToWalletOperation(cashInOutOperation)

        val asset = cashInOutOperation.asset!!
        LOGGER.debug("Processing cash in/out messageId: ${cashInOutContext.messageId} operation (${cashInOutOperation.externalId})" +
                " for client ${cashInOutContext.cashInOutOperation.clientId}, asset ${asset.assetId}," +
                " amount: ${NumberUtils.roundForPrint(walletOperation.amount)}, feeInstructions: $feeInstructions")


        val operations = mutableListOf(walletOperation)

        try {
            cashInOutOperationBusinessValidator.performValidation(cashInOutContext)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, cashInOutOperation.matchingEngineOperationId, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        val fees = try {
            feeProcessor.processFee(feeInstructions, walletOperation, operations, balancesGetter = balancesHolder)
        } catch (e: FeeException) {
            writeErrorResponse(messageWrapper, cashInOutOperation.matchingEngineOperationId, INVALID_FEE, e.message)
            return
        }

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(operations)
        } catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, cashInOutOperation.matchingEngineOperationId, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(cashInOutContext.processedMessage, null, null, sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            writeErrorResponse(messageWrapper, cashInOutOperation.matchingEngineOperationId, RUNTIME, "unable to save balance")
            return
        }
        walletProcessor.apply().sendNotification(cashInOutOperation.externalId!!, MessageType.CASH_IN_OUT_OPERATION.name, messageWrapper.messageId!!)

        publishRabbitMessage(cashInOutContext, fees)

        val outgoingMessage = EventFactory.createCashInOutEvent(walletOperation.amount,
                sequenceNumber,
                cashInOutContext.messageId,
                cashInOutOperation.externalId,
                now,
                MessageType.CASH_IN_OUT_OPERATION,
                walletProcessor.getClientBalanceUpdates(),
                walletOperation,
                fees)

        messageSender.sendMessage(outgoingMessage)

        writeResponse(messageWrapper, cashInOutOperation.matchingEngineOperationId, OK)


        LOGGER.info("Cash in/out walletOperation (${cashInOutOperation.externalId}) for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                "asset ${cashInOutOperation.asset.assetId}, " +
                "amount: ${NumberUtils.roundForPrint(walletOperation.amount)} processed")
    }

    private fun publishRabbitMessage(cashInOutContext: CashInOutContext,
                                     fees: List<Fee>) {
        val cashInOutOperation = cashInOutContext.cashInOutOperation
        val asset = cashInOutOperation.asset
        rabbitCashInOutQueue.put(CashOperation(
                cashInOutOperation.externalId!!,
                cashInOutOperation.clientId,
                cashInOutOperation.dateTime,
                NumberUtils.setScaleRoundHalfUp(cashInOutOperation.amount, asset!!.accuracy).toPlainString(),
                asset.assetId,
                cashInOutContext.messageId,
                fees
        ))
    }

    fun writeResponse(messageWrapper: MessageWrapper, matchingEngineOperationId: String, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(matchingEngineOperationId)
                .setStatus(status.type))
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   matchingEngineOperationId: String,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        val context = messageWrapper.context as CashInOutContext
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(matchingEngineOperationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash in/out operation (${context.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${context.cashInOutOperation.clientId}, " +
                "asset ${context.cashInOutOperation.asset!!.assetId}, amount: ${NumberUtils.roundForPrint(context.cashInOutOperation.amount)}: $errorMessage")
    }
}