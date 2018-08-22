package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage

open class JsonSerializable: OutgoingMessage {
    override fun isNewMessageFormat() = false
}