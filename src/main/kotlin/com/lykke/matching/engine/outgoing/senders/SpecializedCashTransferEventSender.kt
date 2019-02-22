package com.lykke.matching.engine.outgoing.senders

import com.lykke.matching.engine.outgoing.messages.CashTransferEventData

interface SpecializedCashTransferEventSender {
    fun sendEvent(cashTransferEventData: CashTransferEventData)
}