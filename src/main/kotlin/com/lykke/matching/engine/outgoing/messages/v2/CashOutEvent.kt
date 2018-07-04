package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class CashOutEvent(header: Header,
                   val balanceUpdates: List<BalanceUpdate>,
                   val cashOut: CashOut) : AbstractEvent<OutgoingMessages.CashOutEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.CashOutEvent {
        val builder = OutgoingMessages.CashOutEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashOut(cashOut.createGeneratedMessageBuilder())
        return builder.build()
    }

}

