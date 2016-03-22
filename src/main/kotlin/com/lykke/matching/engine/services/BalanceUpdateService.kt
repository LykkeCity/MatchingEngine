package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger

class BalanceUpdateService(val cashOperationService: CashOperationService): AbsractService<ProtocolMessages.BalanceUpdate> {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing balance update for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount}")
        cashOperationService.updateBalance(message.clientId, message.assetId, message.amount)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount}")
    }

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }
}