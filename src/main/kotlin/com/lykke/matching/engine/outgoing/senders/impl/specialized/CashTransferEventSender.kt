package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class CashTransferEventSender(val messageSender: MessageSender): SpecializedEventSender {
    override fun getProcessedMessageClass(): Class<*> {
        return CashTransferEventData::class.java
    }

    override fun sendEvent(outgoingEventData: OutgoingEventData) {
        val cashTransferEventData = outgoingEventData.eventData as CashTransferEventData
        val outgoingMessage = EventFactory.createCashTransferEvent(cashTransferEventData.sequenceNumber,
                cashTransferEventData.messageId,
                cashTransferEventData.transferOperation.externalId,
                cashTransferEventData.now,
                MessageType.CASH_TRANSFER_OPERATION,
                cashTransferEventData.walletProcessor.getClientBalanceUpdates(),
                cashTransferEventData.transferOperation,
                cashTransferEventData.fees)

        messageSender.sendMessage(outgoingMessage)
    }
}