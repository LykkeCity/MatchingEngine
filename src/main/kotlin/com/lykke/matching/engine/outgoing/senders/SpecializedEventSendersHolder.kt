package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.daos.OutgoingEventData

interface SpecializedEventSendersHolder {
    fun <T: OutgoingEventData> getSenders(event: T): List<SpecializedEventSender<T>>
}