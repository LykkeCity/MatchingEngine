package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import java.util.Date

class Header(val messageType: MessageType,
             val sequenceNumber: Long,
             val messageId: String,
             val requestId: String,
             val version: String,
             val timestamp: Date,
             val eventType: String) : AbstractEventPart<OutgoingMessages.Header.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.Header.Builder {
        val builder = OutgoingMessages.Header.newBuilder()
        builder.setMessageType(messageType.id)
                .setSequenceNumber(sequenceNumber)
                .setMessageId(messageId)
                .setRequestId(requestId)
                .setVersion(version)
                .setTimestamp(timestamp.createProtobufTimestampBuilder())
                .eventType = eventType
        return builder
    }
}