package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.Fee
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
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
        LOGGER.debug("Processing cash in/out operation (${message.id}) " +
                "for client ${message.clientId}, asset ${message.assetId}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume)}, feeInstructions: $feeInstructions")

        val  walletOperation = getWalletOperation(message)
        val operations = mutableListOf(walletOperation)

        try {
            cashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            LOGGER.info(e.message)
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        val fees = try {
            feeProcessor.processFee(feeInstructions, walletOperation, operations)
        } catch (e: FeeException) {
            writeInvalidFeeResponse(messageWrapper, walletOperation.id, e.message)
            return
        }

        try {
            balancesHolder.createWalletProcessor(LOGGER)
                    .preProcess(operations)
                    .apply(message.id, MessageType.CASH_IN_OUT_OPERATION.name)
        }  catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        publishRabbitMessage(message, walletOperation, fees)

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMatchingEngineId(walletOperation.id)
                .setStatus(OK.type).build())

        LOGGER.info("Cash in/out walletOperation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume)} processed")
    }

    private fun publishRabbitMessage(message: ProtocolMessages.CashInOutOperation, walletOperation: WalletOperation, fees: List<Fee>) {
        rabbitCashInOutQueue.put(CashOperation(
                message.id,
                walletOperation.clientId,
                walletOperation.dateTime,
                walletOperation.amount.round(assetsHolder.getAsset(walletOperation.assetId).accuracy),
                walletOperation.assetId,
                fees
        ))
    }

    private fun getWalletOperation(message: ProtocolMessages.CashInOutOperation): WalletOperation {
        val operationId = UUID.randomUUID().toString()
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
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setStatus(status.type).build())
    }

    private fun writeInvalidFeeResponse(messageWrapper: MessageWrapper, operationId: String, errorMessage: String = "invalid fee for client") {
        writeErrorResponse(messageWrapper,
                operationId,
                MessageStatus.INVALID_FEE,
                errorMessage)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String = "") {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage)
                .build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage")
    }
}