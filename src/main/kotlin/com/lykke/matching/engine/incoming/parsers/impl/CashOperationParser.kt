package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import java.math.BigDecimal
import java.util.*

class CashOperationParser : ContextParser<CashOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): CashOperationParsedData {
        val message = ProtocolMessages.CashOperation.parseFrom(messageWrapper.byteArray)

        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.bussinesId
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid.toString()

        messageWrapper.context = CashOperationContext(message.uid.toString(),
                messageWrapper.messageId!!,
                getWalletOperation(message),
                message.bussinesId,
                message.clientId
        )

        return CashOperationParsedData(messageWrapper)
    }

    private fun getWalletOperation(message: ProtocolMessages.CashOperation): WalletOperation {
        return WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), BigDecimal.valueOf(message.amount), BigDecimal.ZERO)
    }
}