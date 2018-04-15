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
import com.lykke.matching.engine.utils.NumberUtils
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
        LOGGER.debug("""Processing cash in/out operation (${message.id})
            |for client ${message.clientId}, asset ${message.assetId},
            |amount: ${NumberUtils.roundForPrint(message.volume)}, feeInstructions: $feeInstructions""".trimMargin())

        val operationId = UUID.randomUUID().toString()
        val operation = WalletOperation(operationId, message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)
        val operations = mutableListOf(operation)

        performValidation(messageWrapper = messageWrapper,
                operation = operation,
                feeInstructions = feeInstructions)

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

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.volume)} processed")
    }

    private fun getFirstValidationError(message: ProtocolMessages.CashInOutOperation,
                                        feeInstructions: List<NewFeeInstruction>, assetId: String): CashInOutOperationService.ValidationErrors? {
        if (!checkFee(null, feeInstructions)) {
            return ValidationErrors.INVALID_FEE
        }

        if (message.volume < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            LOGGER.info("""Cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId},
                |volume: ${NumberUtils.roundForPrint(message.volume)}: disabled asset""".trimMargin())
            return ValidationErrors.DISABLED_ASSET
        }

        if (message.volume < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (NumberUtils.parseDouble(balance - reservedBalance + message.volume, assetsHolder.getAsset(assetId).accuracy).toDouble() < 0.0) {
                LOGGER.info("""Cash out operation (${message.id})
                    for client ${message.clientId} asset ${message.assetId},
                    volume: ${NumberUtils.roundForPrint(message.volume)}: low balance $balance, reserved balance $reservedBalance""")
                return ValidationErrors.LOW_BALANCE
            }
        }

        return null
    }

    private fun performValidation(messageWrapper: MessageWrapper,
                                  operation: WalletOperation,
                                  feeInstructions: List<NewFeeInstruction>) {

        val message = messageWrapper.messageId as ProtocolMessages.CashInOutOperation
        val firstValidationError = getFirstValidationError(message, feeInstructions, operation.assetId)

        when (firstValidationError) {
            ValidationErrors.INVALID_FEE -> writeInvalidFeeResponse(messageWrapper, message, operation.id)

            ValidationErrors.DISABLED_ASSET -> messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(message.id)
                    .setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.DISABLED_ASSET.type).build())

            ValidationErrors.LOW_BALANCE -> messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(message.id)
                    .setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.LOW_BALANCE.type).build())
        }
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
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
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
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage)
                .build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage")
        return
    }

    private enum class ValidationErrors {
        INVALID_FEE, DISABLED_ASSET, LOW_BALANCE
    }
}