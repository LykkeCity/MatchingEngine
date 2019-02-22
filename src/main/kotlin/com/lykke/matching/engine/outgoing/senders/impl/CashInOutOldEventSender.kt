package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedCashInOutEventSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class CashInOutOldEventSender(private val rabbitCashInOutQueue: BlockingQueue<CashOperation>): SpecializedCashInOutEventSender {
    override fun sendEvent(eventData: CashInOutEventData) {
        eventData.walletProcessor.sendNotification(eventData.externalId, MessageType.CASH_IN_OUT_OPERATION.name, eventData.messageId)

        rabbitCashInOutQueue.put(CashOperation(
                eventData.externalId,
                eventData.walletOperation.clientId,
                eventData.timestamp,
                NumberUtils.setScaleRoundHalfUp(eventData.walletOperation.amount, eventData.asset.accuracy).toPlainString(),
                eventData.asset.assetId,
                eventData.messageId,
                eventData.internalFees
        ))
    }
}