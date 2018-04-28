package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date

class ReservedBalanceUpdateService(private val balancesHolder: BalancesHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedBalanceUpdateService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = messageWrapper.parsedMessage as ProtocolMessages.ReservedBalanceUpdate
        LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${RoundingUtils.roundForPrint(message.reservedAmount)}")

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        if (message.reservedAmount > balance) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type))
            LOGGER.info("Balance (client ${message.clientId}, asset ${message.assetId}, ${RoundingUtils.roundForPrint(balance)}) is lower that reserved balance ${RoundingUtils.roundForPrint(message.reservedAmount)}")
            return
        }

        val currentReservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        balancesHolder.updateReservedBalance(message.clientId, message.assetId, message.reservedAmount, false)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid,
                MessageType.RESERVED_BALANCE_UPDATE.name,
                Date(),
                listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, balance, currentReservedBalance, message.reservedAmount)), messageWrapper.messageId!!))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(MessageStatus.OK.type))
        LOGGER.debug("Reserved balance updated for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${RoundingUtils.roundForPrint(message.reservedAmount)}")
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedBalanceUpdate {
        return ProtocolMessages.ReservedBalanceUpdate.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        LOGGER.info("Parsed message ${ReservedBalanceUpdateService::class.java.name} messageId ${messageWrapper.messageId}")
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }
}