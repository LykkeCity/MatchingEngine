package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType

data class LimitOrderCancelOperationContext(
        val uid: String,
        val messageId: String,
        val processedMessage: ProcessedMessage,
        val limitOrderIds: Set<String>,
        val messageType: MessageType)