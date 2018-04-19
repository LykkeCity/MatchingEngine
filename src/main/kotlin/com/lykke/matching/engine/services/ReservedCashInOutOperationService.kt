package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class ReservedCashInOutOperationService(private val assetsHolder: AssetsHolder,
                                        private val balancesHolder: BalancesHolder,
                                        private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = getMessage(messageWrapper)
        LOGGER.debug("""Processing reserved cash in/out operation (${message.id})
            | for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.reservedVolume)}""".trimMargin())

        val operation = WalletOperation(UUID.randomUUID().toString(), message.id, message.clientId, message.assetId,
                Date(message.timestamp), 0.0, message.reservedVolume)

        if (!performValidation(messageWrapper, operation)) {
            return
        }

        val accuracy = assetsHolder.getAsset(operation.assetId).accuracy

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(listOf(operation)).apply(message.id, MessageType.RESERVED_CASH_IN_OUT_OPERATION.name)
        } catch (e: BalanceException) {
            LOGGER.info("Reserved cash in/out operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, operation, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        rabbitCashInOutQueue.put(ReservedCashOperation(message.id, operation.clientId, operation.dateTime, operation.reservedAmount.round(accuracy), operation.assetId))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(MessageStatus.OK.type).build())
        LOGGER.info("""Reserved cash in/out operation (${message.id}) for client ${message.clientId},
            |asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.reservedVolume)} processed""".trimMargin())
    }

    private fun isReservedVolumeValid(messageWrapper: MessageWrapper,  reservedBalance: Double,  operation: WalletOperation): Boolean {
        val message = getMessage(messageWrapper)
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        if (NumberUtils.parseDouble(balance - reservedBalance - message.reservedVolume, accuracy).toDouble() < 0.0) {
            writeErrorResponse(messageWrapper, operation, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE)
            LOGGER.info("""Reserved cash in operation (${message.id}) for client ${message.clientId} asset ${message.assetId},
                |volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: low balance $balance, current reserved balance $reservedBalance""".trimMargin())
            return false
        }

        return true
    }

    private fun isBalanceValid(messageWrapper: MessageWrapper, reservedBalance: Double, operation: WalletOperation): Boolean {
        val message = getMessage(messageWrapper)
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy

        if (NumberUtils.parseDouble(reservedBalance + message.reservedVolume, accuracy).toDouble() < 0.0) {
            writeErrorResponse(messageWrapper, operation, MessageStatus.LOW_BALANCE)
            LOGGER.info("""Reserved cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId},
                |volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: low reserved balance $reservedBalance""".trimMargin())
            return false
        }

        return true
    }

    private fun performValidation(messageWrapper: MessageWrapper, operation: WalletOperation): Boolean {
        val message = getMessage(messageWrapper)
        val validations = mutableListOf({isAccuracyValid(messageWrapper, message.assetId, message.reservedVolume, operation)})

        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        if (message.reservedVolume < 0) {
            validations.add({isBalanceValid(messageWrapper, reservedBalance,  operation)})
        } else {
            validations.add({isReservedVolumeValid(messageWrapper, reservedBalance, operation)})
        }

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper, assetId: String, volume: Double, operation: WalletOperation): Boolean {
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(volume, assetsHolder.getAsset(assetId).accuracy)

        if (!volumeValid) {
            writeErrorResponse(messageWrapper, operation, MessageStatus.INVALID_VOLUME_ACCURACY)
        }

        return volumeValid
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper, operation: WalletOperation, status: MessageStatus, errorMessage: String = StringUtils.EMPTY) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operation.id)
                .setStatus(status.type).setStatusReason(errorMessage).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedCashInOutOperation {
        return ProtocolMessages.ReservedCashInOutOperation.parseFrom(array)
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

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.ReservedCashInOutOperation {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.ReservedCashInOutOperation
        return message
    }

}