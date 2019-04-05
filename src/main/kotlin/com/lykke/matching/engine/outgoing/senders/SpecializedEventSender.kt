package com.lykke.matching.engine.outgoing.senders

interface SpecializedEventSender<T> {
    fun getEventClass(): Class<T>
    fun sendEvent(event: Any)
}