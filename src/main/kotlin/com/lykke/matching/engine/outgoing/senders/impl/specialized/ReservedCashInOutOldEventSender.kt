package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ReservedCashInOutEventData
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.outgoing.senders.impl.OldFormatBalancesSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class ReservedCashInOutOldEventSender(private val reservedCashOperationQueue: BlockingQueue<ReservedCashOperation>,
                                      private val oldFormatBalancesSender: OldFormatBalancesSender) : SpecializedEventSender<ReservedCashInOutEventData> {

    override fun getEventClass(): Class<ReservedCashInOutEventData> {
        return ReservedCashInOutEventData::class.java
    }

    override fun sendEvent(event: ReservedCashInOutEventData) {
        oldFormatBalancesSender.sendBalanceUpdate(id = event.requestId,
                messageId = event.messageId,
                clientBalanceUpdates =  event.walletOperationsProcessor.getClientBalanceUpdates(),
                type = MessageType.RESERVED_CASH_IN_OUT_OPERATION)

        reservedCashOperationQueue.put(ReservedCashOperation(event.requestId,
                event.walletOperation.clientId,
                event.date,
                NumberUtils.setScaleRoundHalfUp(event.walletOperation.reservedAmount, event.accuracy).toPlainString(),
                event.walletOperation.assetId,
                event.messageId))
    }

}