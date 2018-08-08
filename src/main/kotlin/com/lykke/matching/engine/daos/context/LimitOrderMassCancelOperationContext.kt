package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType

class LimitOrderMassCancelOperationContext(val uid: String,
                                           val messageId: String,
                                           val clientId: String,
                                           val processedMessage: ProcessedMessage,
                                           val type: MessageType,
                                           val assetPairId: String?,
                                           val isBuy: Boolean?)