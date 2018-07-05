package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.google.protobuf.GeneratedMessageV3

interface EventPart<T : GeneratedMessageV3.Builder<T>> {
    fun createGeneratedMessageBuilder(): T
}