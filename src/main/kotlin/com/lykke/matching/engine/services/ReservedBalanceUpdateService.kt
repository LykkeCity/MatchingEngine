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
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type).build())
            LOGGER.info("Balance (client ${message.clientId}, asset ${message.assetId}, ${RoundingUtils.roundForPrint(balance)}) is lower that reserved balance ${RoundingUtils.roundForPrint(message.reservedAmount)}")
            return
        }

        val now = Date()
        val currentReservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        balancesHolder.updateReservedBalance(message.clientId, message.assetId, now, message.reservedAmount)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid, MessageType.RESERVED_BALANCE_UPDATE.name, now, listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, balance, currentReservedBalance, message.reservedAmount))))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
        LOGGER.debug("Reserved balance updated for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${RoundingUtils.roundForPrint(message.reservedAmount)}")
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedBalanceUpdate {
        return ProtocolMessages.ReservedBalanceUpdate.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }
}