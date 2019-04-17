package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.OutgoingEventData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.outgoing.senders.impl.OldFormatBalancesSender
import com.lykke.matching.engine.services.BalancesServiceImpl
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.juli.logging.LogFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class CashInOutOldEventSender(private val rabbitCashInOutQueue: BlockingQueue<CashOperation>,
                              private val balanceUpdateQueue: BlockingQueue<BalanceUpdate>) : SpecializedEventSender<CashInOutEventData>,
        OldFormatBalancesSender {

    private companion object {
        val LOGGER = LogFactory.getLog(CashInOutOldEventSender::class.java)
    }

    override fun getEventClass(): Class<CashInOutEventData> {
        return CashInOutEventData::class.java
    }

    override fun sendEvent(event: OutgoingEventData) {
        val cashInOutEventData = event as CashInOutEventData

        sendBalanceUpdate(cashInOutEventData.externalId,
                MessageType.CASH_IN_OUT_OPERATION,
                cashInOutEventData.messageId,
                cashInOutEventData.clientBalanceUpdates)

        rabbitCashInOutQueue.put(CashOperation(
                id = cashInOutEventData.externalId,
                clientId = cashInOutEventData.walletOperation.clientId,
                dateTime = cashInOutEventData.timestamp,
                volume = NumberUtils.setScaleRoundHalfUp(cashInOutEventData.walletOperation.amount, cashInOutEventData.asset.accuracy).toPlainString(),
                asset = cashInOutEventData.asset.assetId,
                messageId = cashInOutEventData.messageId,
                fees = cashInOutEventData.internalFees
        ))
    }

    override fun sendBalanceUpdate(id: String,
                                   type: MessageType,
                                   messageId: String,
                                   clientBalanceUpdates: List<ClientBalanceUpdate>) {
        if (clientBalanceUpdates.isNotEmpty()) {
            val balanceUpdate = BalanceUpdate(id, type.name, Date(), clientBalanceUpdates, messageId)
            LOGGER.info(balanceUpdate.toString())
            balanceUpdateQueue.put(balanceUpdate)
        }
    }
}