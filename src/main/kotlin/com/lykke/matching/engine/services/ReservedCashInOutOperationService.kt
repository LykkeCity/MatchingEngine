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
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
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
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.ReservedCashInOutOperation
        LOGGER.debug("Processing reserved cash in/out messageId: ${messageWrapper.messageId} " +
                "operation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.reservedVolume)}")

        val operation = WalletOperation(UUID.randomUUID().toString(), message.id, message.clientId, message.assetId,
                Date(message.timestamp), BigDecimal.ZERO, BigDecimal.valueOf(message.reservedVolume))

        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        val accuracy = assetsHolder.getAsset(operation.assetId).accuracy
        if (message.reservedVolume < 0) {
            if (RoundingUtils.setScaleRoundHalfUp(reservedBalance + BigDecimal.valueOf(message.reservedVolume), accuracy) < BigDecimal.ZERO) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.LOW_BALANCE.type))
                LOGGER.info("Reserved cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.reservedVolume)}: low reserved balance $reservedBalance")
                return
            }
        } else {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            if (RoundingUtils.setScaleRoundHalfUp(balance - reservedBalance - BigDecimal.valueOf(message.reservedVolume), accuracy) < BigDecimal.ZERO) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE.type))
                LOGGER.info("Reserved cash in operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.reservedVolume)}: low balance $balance, current reserved balance $reservedBalance")
                return
            }
        }

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(listOf(operation)).apply(message.id, MessageType.RESERVED_CASH_IN_OUT_OPERATION.name, messageWrapper.messageId!!)
        } catch (e: BalanceException) {
            LOGGER.info("Reserved cash in/out operation (${message.id}) failed due to invalid balance: ${e.message}")
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.LOW_BALANCE.type).setStatusReason(e.message))
            return
        }

        rabbitCashInOutQueue.put(ReservedCashOperation(message.id,
                operation.clientId,
                operation.dateTime,
                RoundingUtils.setScaleRoundHalfUp(operation.reservedAmount, accuracy).toPlainString(),
                operation.assetId,
                messageWrapper.messageId!!))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operation.id)
                .setStatus(MessageStatus.OK.type))
        LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.reservedVolume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedCashInOutOperation {
        return ProtocolMessages.ReservedCashInOutOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.id
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type)
        )
    }
}