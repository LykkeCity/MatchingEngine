package com.lykke.matching.engine.outgoing.messages.v2.events

import com.google.protobuf.GeneratedMessageV3
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header

abstract class Event<out T : GeneratedMessageV3>(val header: Header) {

    fun sequenceNumber() = header.sequenceNumber

    abstract fun buildGeneratedMessage(): T
}