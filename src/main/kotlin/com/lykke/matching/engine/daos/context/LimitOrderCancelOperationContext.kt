package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.messages.MessageType

data class LimitOrderCancelOperationContext(
        val uid: String,
        val limitOrderIds: Set<String>,
        val messageType: MessageType)