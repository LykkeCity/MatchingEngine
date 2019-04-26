package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class CashInOutEventSender(private val messageSender: MessageSender) : SpecializedEventSender<CashInOutEventData> {
    override fun getEventClass(): Class<CashInOutEventData> {
        return CashInOutEventData::class.java
    }

    override fun sendEvent(event: CashInOutEventData) {
        val outgoingMessage = EventFactory.createCashInOutEvent(volume = event.walletOperation.amount,
                sequenceNumber = event.sequenceNumber,
                messageId = event.messageId,
                requestId = event.externalId,
                date = event.now,
                messageType = MessageType.CASH_IN_OUT_OPERATION,
                clientBalanceUpdates = event.walletProcessor.getClientBalanceUpdates(),
                cashInOperation = event.walletOperation,
                internalFees = event.internalFees)

        messageSender.sendMessage(outgoingMessage)
    }
}