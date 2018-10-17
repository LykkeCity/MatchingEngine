package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.incoming.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.springframework.stereotype.Component
import java.util.*

@Component
class LimitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): LimitOrderMassCancelOperationParsedData {
        messageWrapper.context = parseMessage(messageWrapper)
        return LimitOrderMassCancelOperationParsedData(messageWrapper)
    }

    private fun parseMessage(messageWrapper: MessageWrapper): LimitOrderMassCancelOperationContext {
        val message = ProtocolMessages.LimitOrderMassCancel.parseFrom(messageWrapper.byteArray)

        val messageId = if (message.hasMessageId()) message.messageId else message.uid
        messageWrapper.messageId = messageId
        messageWrapper.id = message.uid
        messageWrapper.timestamp = Date().time
        messageWrapper.processedMessage = ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)

        val messageType =  MessageType.valueOf(messageWrapper.type) ?: throw Exception("Unknown message type ${messageWrapper.type}")

        val assetPairId = if (message.hasAssetPairId()) message.assetPairId else null
        val isBuy = if (message.hasIsBuy()) message.isBuy else null

        return LimitOrderMassCancelOperationContext(message.uid, messageId, message.clientId,
                messageWrapper.processedMessage!!, messageType,
                assetPairId, isBuy)
    }
}