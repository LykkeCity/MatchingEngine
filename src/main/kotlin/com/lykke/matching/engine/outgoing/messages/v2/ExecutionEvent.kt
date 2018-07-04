package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class ExecutionEvent(header: Header,
                     val balanceUpdates: List<BalanceUpdate>?,
                     val orders: List<Order>) : AbstractEvent<OutgoingMessages.ExecutionEvent>(header) {

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