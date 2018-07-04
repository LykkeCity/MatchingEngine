package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.CashIn
import com.lykke.matching.engine.outgoing.messages.v2.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.Header
import com.lykke.matching.engine.outgoing.messages.v2.MessageType

class CashInEventBuilder : EventBuilder<CashInEventData, CashInEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var cashIn: CashIn? = null

    override fun getMessageType() = MessageType.CASH_IN

    override fun setEventData(eventData: CashInEventData): EventBuilder<CashInEventData, CashInEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        cashIn = CashIn(eventData.cashInOperation.clientId,
                eventData.cashInOperation.assetId,
                bigDecimalToString(eventData.cashInOperation.amount)!!,
                convertFees(eventData.internalFees))
        return this
    }

    override fun buildEvent(header: Header) = CashInEvent(header, balanceUpdates!!, cashIn!!)

}