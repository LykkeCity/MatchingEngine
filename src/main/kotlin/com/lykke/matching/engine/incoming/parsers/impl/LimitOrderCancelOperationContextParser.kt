package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.springframework.stereotype.Component
import java.util.*

@Component
class LimitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): LimitOrderCancelOperationParsedData {
        messageWrapper.context = parseContext(messageWrapper)
        return LimitOrderCancelOperationParsedData(messageWrapper)
    }

    private fun parseContext(messageWrapper: MessageWrapper): LimitOrderCancelOperationContext {
        return if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            parserOldMessage(messageWrapper)
        } else {
            parseMessage(messageWrapper)
        }
    }

    private fun parseMessage(messageWrapper: MessageWrapper): LimitOrderCancelOperationContext {
        val message =ProtocolMessages.LimitOrderCancel.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.timestamp = Date().time
        messageWrapper.id = message.uid

        return LimitOrderCancelOperationContext(message.limitOrderIdList.toSet(), message.uid)
    }

    private fun parserOldMessage(messageWrapper: MessageWrapper): LimitOrderCancelOperationContext {
        val message = ProtocolMessages.OldLimitOrderCancel.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.timestamp = Date().time
        messageWrapper.id = message.uid.toString()

        return LimitOrderCancelOperationContext(setOf(message.limitOrderId.toString()), message.uid.toString())
    }
}