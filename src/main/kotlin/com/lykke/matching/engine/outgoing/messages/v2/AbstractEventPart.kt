package com.lykke.matching.engine.outgoing.messages.v2

import com.google.protobuf.GeneratedMessageV3

interface AbstractEventPart<T : GeneratedMessageV3.Builder<T>> {
    fun createGeneratedMessageBuilder(): T
}