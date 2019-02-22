package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedCashInOutEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class CashInOutEventSender(private val messageSender: MessageSender): SpecializedCashInOutEventSender {
    override fun sendEvent(eventData: CashInOutEventData) {
        val outgoingMessage = EventFactory.createCashInOutEvent(eventData.walletOperation.amount,
                eventData.sequenceNumber,
                eventData.messageId,
                eventData.externalId,
                eventData.now,
                MessageType.CASH_IN_OUT_OPERATION,
                eventData.walletProcessor.getClientBalanceUpdates(),
                eventData.walletOperation,
                eventData.internalFees)

        messageSender.sendMessage(outgoingMessage)
    }
}