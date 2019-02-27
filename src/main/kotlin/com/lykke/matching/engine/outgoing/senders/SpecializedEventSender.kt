package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.outgoing.messages.OutgoingEventData

interface SpecializedEventSender {
    fun getEventClass(): Class<*>
    fun sendEvent(eventData: OutgoingEventData)
}