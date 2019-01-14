package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
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
        val message = ProtocolMessages.LimitOrderCancel.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
        messageWrapper.timestamp = Date().time
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)

        return LimitOrderCancelOperationContext(message.uid,
                messageWrapper.messageId!!,
                messageWrapper.processedMessage!!,
                message.limitOrderIdList.toSet(), getMessageType(messageWrapper.type))
    }

    private fun getMessageType(type: Byte): MessageType {
        return MessageType.valueOf(type) ?: throw Exception("Unknown message type $type")
    }
}