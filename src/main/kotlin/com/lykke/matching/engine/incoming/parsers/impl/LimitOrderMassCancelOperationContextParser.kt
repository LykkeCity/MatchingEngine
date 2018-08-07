package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.incoming.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
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

        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
        messageWrapper.id = message.uid
        messageWrapper.timestamp = Date().time

        return LimitOrderMassCancelOperationContext(message.uid, message.clientId, message.assetPairId, message.isBuy)
    }
}