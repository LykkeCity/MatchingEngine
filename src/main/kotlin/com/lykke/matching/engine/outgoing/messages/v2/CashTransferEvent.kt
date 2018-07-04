package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class CashTransferEvent(header: Header,
                        val balanceUpdates: List<BalanceUpdate>,
                        val cashTransfer: CashTransfer) : AbstractEvent<OutgoingMessages.CashTransferEvent>(header) {

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