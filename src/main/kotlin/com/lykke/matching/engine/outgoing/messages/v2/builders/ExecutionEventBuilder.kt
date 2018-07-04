package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.Header
import com.lykke.matching.engine.outgoing.messages.v2.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.Order

class ExecutionEventBuilder: EventBuilder<ExecutionEventData, ExecutionEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var orders: List<Order>? = null

    override fun getMessageType() = MessageType.ORDER

    override fun setEventData(eventData: ExecutionEventData): EventBuilder<ExecutionEventData, ExecutionEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        orders = convertOrders(eventData.limitOrdersWithTrades, eventData.marketOrderWithTrades)
        return this
    }

    override fun buildEvent(header: Header) = ExecutionEvent(header, balanceUpdates, orders!!)
}