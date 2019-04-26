package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.ReservedCashInOutEventData

interface OutgoingEventProcessor {
    fun submitCashTransferEvent(cashTransferEventData: CashTransferEventData)
    fun submitCashInOutEvent(cashInOutEventData: CashInOutEventData)
    fun submitExecutionEvent(executionEventData: ExecutionData)
    fun submitReservedCashInOutEvent(reservedCashInOutEventData: ReservedCashInOutEventData)
}