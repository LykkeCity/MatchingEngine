package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_RESERVED_BALANCE_UPDATE
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.RESERVED_AMOUNT
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date

class ReservedBalanceUpdateService(private val balancesHolder: BalancesHolder) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedBalanceUpdateService::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${RoundingUtils.roundForPrint(message.reservedAmount)}")

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        if (message.reservedAmount > balance) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type).build())
            LOGGER.info("Balance (client ${message.clientId}, asset ${message.assetId}, ${RoundingUtils.roundForPrint(balance)}) is lower that reserved balance ${RoundingUtils.roundForPrint(message.reservedAmount)}")
            return
        }

        val currentReservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        balancesHolder.updateReservedBalance(message.clientId, message.assetId, message.reservedAmount)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid, MessageType.RESERVED_BALANCE_UPDATE.name, Date(), listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, balance, currentReservedBalance, message.reservedAmount))))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
        LOGGER.debug("Reserved balance updated for client ${message.clientId}, asset ${message.assetId}, reserved amount: ${RoundingUtils.roundForPrint(message.reservedAmount)}")

        METRICS_LOGGER.log(Line(ME_RESERVED_BALANCE_UPDATE, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(RESERVED_AMOUNT, message.reservedAmount.toString()) // fixme: or com.lykke.matching.engine.logging.AMOUNT ?
        )))
        METRICS_LOGGER.log(KeyValue(ME_RESERVED_BALANCE_UPDATE, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedBalanceUpdate {
        return ProtocolMessages.ReservedBalanceUpdate.parseFrom(array)
    }
}