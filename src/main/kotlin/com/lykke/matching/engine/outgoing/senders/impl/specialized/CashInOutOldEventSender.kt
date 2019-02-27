package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class CashInOutOldEventSender(private val rabbitCashInOutQueue: BlockingQueue<CashOperation>) : SpecializedEventSender {
    override fun getEventClass(): Class<*> {
        return CashInOutEventData::class.java
    }

    override fun sendEvent(eventData: OutgoingEventData) {
        val cashInOutEventData = eventData.eventData as CashInOutEventData
        cashInOutEventData
                .walletProcessor
                .sendNotification(id = cashInOutEventData.externalId,
                        type = MessageType.CASH_IN_OUT_OPERATION.name,
                        messageId = cashInOutEventData.messageId)

        rabbitCashInOutQueue.put(CashOperation(
                id =  cashInOutEventData.externalId,
                clientId = cashInOutEventData.walletOperation.clientId,
                dateTime = cashInOutEventData.timestamp,
                volume = NumberUtils.setScaleRoundHalfUp(cashInOutEventData.walletOperation.amount, cashInOutEventData.asset.accuracy).toPlainString(),
                asset = cashInOutEventData.asset.assetId,
                messageId = cashInOutEventData.messageId,
                fees = cashInOutEventData.internalFees
        ))
    }
}