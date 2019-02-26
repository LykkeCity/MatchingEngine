package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class CashInOutEventSender(private val messageSender: MessageSender) : SpecializedEventSender {
    override fun getProcessedMessageClass(): Class<*> {
        return CashInOutEventData::class.java
    }

    override fun sendEvent(eventData: OutgoingEventData) {
        val cashInOutEventData = eventData.eventData as CashInOutEventData
        val outgoingMessage = EventFactory.createCashInOutEvent(cashInOutEventData.walletOperation.amount,
                cashInOutEventData.sequenceNumber,
                cashInOutEventData.messageId,
                cashInOutEventData.externalId,
                cashInOutEventData.now,
                MessageType.CASH_IN_OUT_OPERATION,
                cashInOutEventData.walletProcessor.getClientBalanceUpdates(),
                cashInOutEventData.walletOperation,
                cashInOutEventData.internalFees)

        messageSender.sendMessage(outgoingMessage)
    }
}