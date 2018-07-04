package com.lykke.matching.engine.logging

import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage

data class MessageWrapper(val message: OutgoingMessage, val stringValue: String)