package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.daos.ExecutionData

interface SpecializedExecutionEventSender {
    fun sendEvent(executionData: ExecutionData)
}