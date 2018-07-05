package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashTransfer

class CashTransferEvent(header: Header,
                        val balanceUpdates: List<BalanceUpdate>,
                        val cashTransfer: CashTransfer) : Event<OutgoingMessages.CashTransferEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.CashTransferEvent {
        val builder = OutgoingMessages.CashTransferEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashTransfer(cashTransfer.createGeneratedMessageBuilder())
        return builder.build()
    }

}