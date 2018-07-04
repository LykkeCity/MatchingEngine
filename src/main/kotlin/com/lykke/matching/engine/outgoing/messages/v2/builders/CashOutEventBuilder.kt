package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.CashOut
import com.lykke.matching.engine.outgoing.messages.v2.CashOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.Header
import com.lykke.matching.engine.outgoing.messages.v2.MessageType

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