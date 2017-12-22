package com.lykke.matching.engine.deduplication

data class ProcessedMessage(
    val type: Byte,
    val timestamp: Long,
    val messageId: String
) {
    override fun toString(): String {
        return "ProcessedMessage(type=$type, timestamp=$timestamp, messageId='$messageId')"
    }
}