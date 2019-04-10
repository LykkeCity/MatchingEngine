package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.OutgoingEventData
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

    override fun sendEvent(event: OutgoingEventData) {
        val cashInOutEventData = event as CashInOutEventData
        val outgoingMessage = EventFactory.createCashInOutEvent(volume =  cashInOutEventData.walletOperation.amount,
                sequenceNumber = cashInOutEventData.sequenceNumber,
                messageId = cashInOutEventData.messageId,
                requestId = cashInOutEventData.externalId,
                date = cashInOutEventData.now,
                messageType = MessageType.CASH_IN_OUT_OPERATION,
                clientBalanceUpdates = cashInOutEventData.walletProcessor.getClientBalanceUpdates(),
                cashInOperation = cashInOutEventData.walletOperation,
                internalFees = cashInOutEventData.internalFees)

        messageSender.sendMessage(outgoingMessage)
    }
}