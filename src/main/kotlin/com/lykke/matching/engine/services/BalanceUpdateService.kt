package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date

class BalanceUpdateService(private val balancesHolder: BalancesHolder): AbstractService<ProtocolMessages.OldBalanceUpdate> {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            val message = parseOld(messageWrapper.byteArray)
            LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")


            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

            balancesHolder.updateBalance(message.clientId, message.assetId, message.amount)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid.toString(), MessageType.BALANCE_UPDATE.name, Date(), listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, message.amount, reservedBalance, reservedBalance))))

            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")
        } else {
            val message = parse(messageWrapper.byteArray)
            LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")

            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (reservedBalance > message.amount) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type).build())
                LOGGER.info("Balance (client ${message.clientId}, asset ${message.assetId}, ${RoundingUtils.roundForPrint(message.amount)} is lower that reserved balance ${RoundingUtils.roundForPrint(message.amount)}")
                return
            }

            balancesHolder.updateBalance(message.clientId, message.assetId, message.amount)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid, MessageType.BALANCE_UPDATE.name, Date(), listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, message.amount, reservedBalance, reservedBalance))))

            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")
        }
    }

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }

    private fun parseOld(array: ByteArray): ProtocolMessages.OldBalanceUpdate {
        return ProtocolMessages.OldBalanceUpdate.parseFrom(array)
    }
}