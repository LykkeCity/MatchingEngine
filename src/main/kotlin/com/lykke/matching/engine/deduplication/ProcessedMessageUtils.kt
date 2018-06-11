package com.lykke.matching.engine.deduplication

import com.lykke.matching.engine.messages.MessageType

class ProcessedMessageUtils {
    companion object {
        private val MESSAGE_TYPES_DEDUPLICATION_NOT_NEEDED = setOf(MessageType.MULTI_LIMIT_ORDER,
                MessageType.OLD_MULTI_LIMIT_ORDER,
                MessageType.MULTI_LIMIT_ORDER_CANCEL)

        fun isDeduplicationNotNeeded(type: Byte): Boolean {
            return MESSAGE_TYPES_DEDUPLICATION_NOT_NEEDED.contains(MessageType.Companion.valueOf(type))
        }
    }
}