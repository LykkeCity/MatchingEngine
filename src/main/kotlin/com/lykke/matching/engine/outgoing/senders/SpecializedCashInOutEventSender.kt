package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.outgoing.messages.CashInOutEventData

interface SpecializedCashInOutEventSender {
    fun sendEvent(eventData: CashInOutEventData)
}