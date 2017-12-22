package com.lykke.matching.engine.deduplication

import java.io.Serializable

data class ProcessedMessage(
    val type: Byte,
    val timestamp: Long,
    val messageId: String
): Serializable {
    override fun toString(): String {
        return "ProcessedMessage(type=$type, timestamp=$timestamp, messageId='$messageId')"
    }
}