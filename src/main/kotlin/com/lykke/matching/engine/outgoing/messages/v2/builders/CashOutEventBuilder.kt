package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashOut
import com.lykke.matching.engine.outgoing.messages.v2.events.CashOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType

class CashOutEventBuilder : EventBuilder<CashOutEventData, CashOutEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var cashOut: CashOut? = null

    override fun getMessageType() = MessageType.CASH_OUT

    override fun setEventData(eventData: CashOutEventData): EventBuilder<CashOutEventData, CashOutEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        cashOut = CashOut(eventData.cashOutOperation.clientId,
                eventData.cashOutOperation.assetId,
                bigDecimalToString(eventData.cashOutOperation.amount.abs())!!,
                convertFees(eventData.internalFees))
        return this
    }

    override fun buildEvent(header: Header) = CashOutEvent(header, balanceUpdates!!, cashOut!!)

}