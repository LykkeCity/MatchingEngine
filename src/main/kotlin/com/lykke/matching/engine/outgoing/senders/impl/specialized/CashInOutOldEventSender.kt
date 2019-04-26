package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class CashInOutOldEventSender(private val rabbitCashInOutQueue: BlockingQueue<CashOperation>) : SpecializedEventSender<CashInOutEventData> {
    override fun getEventClass(): Class<CashInOutEventData> {
        return CashInOutEventData::class.java
    }

    override fun sendEvent(event: CashInOutEventData) {
        event
                .walletProcessor
                .sendNotification(id = event.externalId,
                        type = MessageType.CASH_IN_OUT_OPERATION.name,
                        messageId = event.messageId)

        rabbitCashInOutQueue.put(CashOperation(
                id = event.externalId,
                clientId = event.walletOperation.clientId,
                dateTime = event.timestamp,
                volume = NumberUtils.setScaleRoundHalfUp(event.walletOperation.amount, event.asset.accuracy).toPlainString(),
                asset = event.asset.assetId,
                messageId = event.messageId,
                fees = event.internalFees
        ))
    }
}