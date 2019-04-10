package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.daos.OutgoingEventData

interface SpecializedEventSender<T : OutgoingEventData> {
    fun getEventClass(): Class<T>
    fun sendEvent(event: OutgoingEventData)
}