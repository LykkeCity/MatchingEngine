package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.checkFee
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
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                private val assetsHolder: AssetsHolder,
                                private val balancesHolder: BalancesHolder,
                                private val applicationSettingsCache: ApplicationSettingsCache,
                                private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>,
                                private val feeProcessor: FeeProcessor) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        val feeInstructions = NewFeeInstruction.create(message.feesList)
        LOGGER.debug("Processing cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}, feeInstructions: $feeInstructions")

        val operationId = UUID.randomUUID().toString()
        if (!checkFee(null, feeInstructions)) {
            writeInvalidFeeResponse(messageWrapper, message, operationId)
            return
        }

        val operation = WalletOperation(operationId, message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)
        val operations = mutableListOf(operation)

        if (message.volume < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(message.id)
                    .setMessageId(messageWrapper.messageId)
                    .setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.DISABLED_ASSET.type)
                    .build())
            LOGGER.info("Cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)}: disabled asset")
            return
        }

        if (message.volume < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (RoundingUtils.parseDouble(balance - reservedBalance + message.volume, assetsHolder.getAsset(operation.assetId).accuracy).toDouble() < 0.0) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setId(message.id)
                        .setMessageId(messageWrapper.messageId)
                        .setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.LOW_BALANCE.type)
                        .build())
                LOGGER.info("Cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)}: low balance $balance, reserved balance $reservedBalance")
                return
            }
        }

        val fees = try {
            feeProcessor.processFee(feeInstructions, operation, operations)
        } catch (e: FeeException) {
            writeInvalidFeeResponse(messageWrapper, message, operationId, e.message)
            return
        }

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(operations).apply(message.id, MessageType.CASH_IN_OUT_OPERATION.name)
        }  catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, message, operationId, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        rabbitCashInOutQueue.put(CashOperation(
                message.id,
                operation.clientId,
                operation.dateTime,
                operation.amount.round(assetsHolder.getAsset(operation.assetId).accuracy),
                operation.assetId,
                fees
        ))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMessageId(messageWrapper.messageId)
                .setMatchingEngineId(operation.id)
                .setStatus(OK.type)
                .build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashInOutOperation {
        return ProtocolMessages.CashInOutOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMessageId(messageWrapper.messageId)
                .setStatus(status.type)
                .build())
    }

    private fun writeInvalidFeeResponse(messageWrapper: MessageWrapper, message: ProtocolMessages.CashInOutOperation, operationId: String, errorMessage: String = "invalid fee for client") {
        writeErrorResponse(messageWrapper, message, operationId, MessageStatus.INVALID_FEE, errorMessage)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   message: ProtocolMessages.CashInOutOperation,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMessageId(messageWrapper.messageId)
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage)
                .build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}: $errorMessage")
        return
    }
}