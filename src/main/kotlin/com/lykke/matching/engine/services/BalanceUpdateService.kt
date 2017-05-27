package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_BALANCE_UPDATE
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.concurrent.BlockingQueue

class BalanceUpdateService(private val balancesHolder: BalancesHolder,
                           private val balanceUpdateQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.BalanceUpdate> {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        balancesHolder.updateBalance(message.clientId, message.assetId, message.amount)

        balanceUpdateQueue.put(BalanceUpdate(message.uid.toString(), MessageType.BALANCE_UPDATE.name, Date(),
                listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, balancesHolder.getBalance(message.clientId, message.assetId)))))

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")
        
        METRICS_LOGGER.log(Line(ME_BALANCE_UPDATE, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(AMOUNT, message.amount.toString())
        )))
        METRICS_LOGGER.log(KeyValue(ME_BALANCE_UPDATE, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }
}