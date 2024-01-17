package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate

@Deprecated("Old outgoing messages format is deprecated")
interface OldFormatBalancesSender {
    fun sendBalanceUpdate(id: String,
                          type: MessageType,
                          messageId: String,
                          clientBalanceUpdates: List<ClientBalanceUpdate>)
}