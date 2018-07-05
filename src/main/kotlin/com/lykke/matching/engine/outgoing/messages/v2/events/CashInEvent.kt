package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashIn
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header

class CashInEvent(header: Header,
                  val balanceUpdates: List<BalanceUpdate>,
                  val cashIn: CashIn) : Event<OutgoingMessages.CashInEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.CashInEvent {
        val builder = OutgoingMessages.CashInEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashIn(cashIn.createGeneratedMessageBuilder())
        return builder.build()
    }

}