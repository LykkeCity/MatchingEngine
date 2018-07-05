package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashOut

class CashOutEvent(header: Header,
                   val balanceUpdates: List<BalanceUpdate>,
                   val cashOut: CashOut) : Event<OutgoingMessages.CashOutEvent>(header) {

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

