package com.lykke.matching.engine.outgoing.messages.v2

interface OutgoingMessage {
    fun isNewMessageFormat(): Boolean = true
}