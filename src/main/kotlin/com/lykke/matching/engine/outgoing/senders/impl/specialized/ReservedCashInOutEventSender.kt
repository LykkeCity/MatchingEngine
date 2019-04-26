package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ReservedCashInOutEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class ReservedCashInOutEventSender(private val messageSender: MessageSender) : SpecializedEventSender<ReservedCashInOutEventData> {

    override fun getEventClass(): Class<ReservedCashInOutEventData> {
        return ReservedCashInOutEventData::class.java
    }

    override fun sendEvent(event: ReservedCashInOutEventData) {
        val outgoingMessage = EventFactory.createReservedBalanceUpdateEvent(event.sequenceNumber,
                event.messageId,
                event.requestId,
                event.date,
                MessageType.RESERVED_CASH_IN_OUT_OPERATION,
                event.clientBalanceUpdates,
                event.walletOperation)

        messageSender.sendMessage(outgoingMessage)
    }

}