package com.lykke.matching.engine.outgoing.messages.v2

import com.google.protobuf.Timestamp
import java.util.Date

fun Date.createProtobufTimestampBuilder(): Timestamp.Builder {
    val instant = this.toInstant()
    return Timestamp.newBuilder()
            .setSeconds(instant.epochSecond)
            .setNanos(instant.nano)
}