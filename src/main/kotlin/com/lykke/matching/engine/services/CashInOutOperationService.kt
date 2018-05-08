package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
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

class CashInOutOperationService(private val assetsHolder: AssetsHolder,
                                private val balancesHolder: BalancesHolder,
                                private val applicationSettingsCache: ApplicationSettingsCache,
                                private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>,
                                private val feeProcessor: FeeProcessor) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)

        val feeInstructions = NewFeeInstruction.create(message.feesList)
        LOGGER.debug("""Processing cash in/out operation (${message.id})
            |for client ${message.clientId}, asset ${message.assetId},
            |amount: ${NumberUtils.roundForPrint(message.volume)}, feeInstructions: $feeInstructions""".trimMargin())

        val  walletOperation = getWalletOperation(message)

        val operations = mutableListOf(walletOperation)

        val valid = performValidation(messageWrapper = messageWrapper,
                walletOperationId = walletOperation.id,
                feeInstructions = feeInstructions)

        if (!valid) {
            return
        }

        val fees = try {
            feeProcessor.processFee(feeInstructions, walletOperation, operations)
        } catch (e: FeeException) {
            writeInvalidFeeResponse(messageWrapper, walletOperation.id, e.message)
            return
        }

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(operations).apply(message.id, MessageType.CASH_IN_OUT_OPERATION.name)
        }  catch (e: BalanceException) {
            writeErrorResponse(messageWrapper, walletOperation.id, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        rabbitCashInOutQueue.put(CashOperation(
                message.id,
                walletOperation.clientId,
                walletOperation.dateTime,
                walletOperation.amount.round(assetsHolder.getAsset(walletOperation.assetId).accuracy),
                walletOperation.assetId,
                fees
        ))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMatchingEngineId(walletOperation.id)
                .setStatus(OK.type).build())

        LOGGER.info("""Cash in/out walletOperation (${message.id}) for client ${message.clientId},
            | asset ${message.assetId},
            | amount: ${NumberUtils.roundForPrint(message.volume)} processed""".trimMargin())
    }

    private fun getWalletOperation(message: ProtocolMessages.CashInOutOperation): WalletOperation {
        val operationId = UUID.randomUUID().toString()
        return WalletOperation(operationId, message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)
    }

    private fun performValidation(messageWrapper: MessageWrapper,
                                        walletOperationId: String,
                                        feeInstructions: List<NewFeeInstruction>): Boolean {

        val validations = arrayOf({ isFeeValid(feeInstructions, messageWrapper, walletOperationId) },
                { isAssetEnabled(messageWrapper, walletOperationId) },
                { isBalanceValid(messageWrapper, walletOperationId) },
                { isAccuracyValid(messageWrapper, walletOperationId) })

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun isBalanceValid(messageWrapper: MessageWrapper, walletOperationId: String): Boolean {
        val message = getMessage(messageWrapper)
        if (message.volume < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (NumberUtils.parseDouble(balance - reservedBalance + message.volume, assetsHolder.getAsset(message.assetId).accuracy).toDouble() < 0.0) {
                LOGGER.info("""Cash out operation (${message.id})
                        for client ${message.clientId} asset ${message.assetId},
                        volume: ${NumberUtils.roundForPrint(message.volume)}: low balance $balance, reserved balance $reservedBalance""")
                writeErrorResponse(messageWrapper, walletOperationId, MessageStatus.LOW_BALANCE)
                return false
            }
        }

        return true
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper, walletOperationId: String): Boolean {
        val message = getMessage(messageWrapper)

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(message.volume, assetsHolder.getAsset(message.assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy is invalid  client: ${message.clientId}, asset: ${message.assetId}, volume: $message.volume")
            writeErrorResponse(messageWrapper, walletOperationId, MessageStatus.INVALID_VOLUME_ACCURACY)
        }

        return volumeValid
    }

    private fun isAssetEnabled(messageWrapper: MessageWrapper, walletOperationId: String): Boolean {
        val message = getMessage(messageWrapper)
        if (message.volume < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            LOGGER.info("""Cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId},
                    |volume: ${NumberUtils.roundForPrint(message.volume)}: disabled asset""".trimMargin())
            writeErrorResponse(messageWrapper, walletOperationId, MessageStatus.DISABLED_ASSET)
            return false
        }

        return true
    }

    private fun isFeeValid(feeInstructions: List<NewFeeInstruction>, messageWrapper: MessageWrapper, walletOperationId: String): Boolean {
        if (!checkFee(null, feeInstructions)) {
            writeInvalidFeeResponse(messageWrapper, walletOperationId)
            return false
        }
        return true
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
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.volume)}: $errorMessage")
        return
    }
}