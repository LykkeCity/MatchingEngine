package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.outgoing.messages.v2.CashInOutEventData

interface SpecializedCashInOutEventSender {
    fun sendEvent(eventData: CashInOutEventData)
}