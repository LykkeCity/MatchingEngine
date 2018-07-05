package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Order
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate

class ExecutionEvent(header: Header,
                     val balanceUpdates: List<BalanceUpdate>?,
                     val orders: List<Order>) : Event<OutgoingMessages.ExecutionEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.ExecutionEvent {
        val builder = OutgoingMessages.ExecutionEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates?.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        orders.forEach { order ->
            builder.addOrders(order.createGeneratedMessageBuilder())
        }
        return builder.build()
    }

}