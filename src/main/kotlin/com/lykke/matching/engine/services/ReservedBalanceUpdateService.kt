package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date

class ReservedBalanceUpdateService(private val balancesHolder: BalancesHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedBalanceUpdateService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing holders update messageId: ${messageWrapper.messageId} " +
                "for client ${message.clientId}, asset ${message.assetId}, " +
                "reserved amount: ${NumberUtils.roundForPrint(message.reservedAmount)}")

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        if (BigDecimal.valueOf(message.reservedAmount) > balance) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type))
            LOGGER.info("Balance (client ${message.clientId}, asset ${message.assetId}, ${NumberUtils.roundForPrint(balance)}) is lower that reserved balance ${NumberUtils.roundForPrint(message.reservedAmount)}")
            return
        }

        val currentReservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        balancesHolder.updateReservedBalance(message.clientId, message.assetId, BigDecimal.valueOf(message.reservedAmount), false)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid,
                MessageType.RESERVED_BALANCE_UPDATE.name,
                Date(),
                listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, balance, currentReservedBalance,  BigDecimal.valueOf(message.reservedAmount))), messageWrapper.messageId!!))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(MessageStatus.OK.type))
        LOGGER.debug("Reserved balance updated for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${NumberUtils.roundForPrint(message.reservedAmount)}")
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage as ProtocolMessages.ReservedBalanceUpdate

    private fun parse(array: ByteArray): ProtocolMessages.ReservedBalanceUpdate {
        return ProtocolMessages.ReservedBalanceUpdate.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }
}