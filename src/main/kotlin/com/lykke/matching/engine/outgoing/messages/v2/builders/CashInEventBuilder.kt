package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashIn
import com.lykke.matching.engine.outgoing.messages.v2.events.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType

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