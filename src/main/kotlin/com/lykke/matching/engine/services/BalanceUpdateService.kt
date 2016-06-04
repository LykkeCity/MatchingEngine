package com.lykke.matching.engine.services

import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_BALANCE_UPDATE
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger
import java.time.LocalDateTime

class BalanceUpdateService(val cashOperationService: CashOperationService): AbsractService<ProtocolMessages.BalanceUpdate> {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing balance update for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount}")
        cashOperationService.updateBalance(message.clientId, message.assetId, message.amount)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount}")
        
        METRICS_LOGGER.log(Line(ME_BALANCE_UPDATE, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(AMOUNT, message.amount.toString())
        )))
    }

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }
}