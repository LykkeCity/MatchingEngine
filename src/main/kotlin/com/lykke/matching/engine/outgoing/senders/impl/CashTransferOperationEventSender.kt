package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedCashTransferEventSender
import com.lykke.matching.engine.services.MessageSender
import org.springframework.stereotype.Component

@Component
class CashTransferOperationEventSender(val messageSender: MessageSender): SpecializedCashTransferEventSender {

    override fun sendEvent(cashTransferEventData: CashTransferEventData) {
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