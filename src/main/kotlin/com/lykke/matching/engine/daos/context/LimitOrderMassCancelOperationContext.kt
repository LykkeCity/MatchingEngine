package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType

data class LimitOrderMassCancelOperationContext(val uid: String,
                                           val messageId: String,
                                           val clientId: String,
                                           val processedMessage: ProcessedMessage,
                                           val messageType: MessageType,
                                           val assetPairId: String?,
                                           val isBuy: Boolean?)