package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.ReservedBalanceUpdate

class ReservedBalanceUpdateEvent(header: Header,
                                 val balanceUpdates: List<BalanceUpdate>,
                                 val reservedBalanceUpdate: ReservedBalanceUpdate): Event<OutgoingMessages.ReservedBalanceUpdateEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.ReservedBalanceUpdateEvent {
        val builder = OutgoingMessages.ReservedBalanceUpdateEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setReservedBalanceUpdate(reservedBalanceUpdate.createGeneratedMessageBuilder())
        return builder.build()
    }
}