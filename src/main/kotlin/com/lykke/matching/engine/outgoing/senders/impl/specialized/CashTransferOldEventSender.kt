package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.outgoing.senders.impl.OldFormatBalancesSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import java.util.concurrent.BlockingQueue

@Component
@Deprecated("Old format of outgoing message is deprecated")
class CashTransferOldEventSender(private val notificationQueue: BlockingQueue<CashTransferOperation>,
                                 private val oldFormatBalancesSender: OldFormatBalancesSender) : SpecializedEventSender<CashTransferEventData> {
    override fun getEventClass(): Class<CashTransferEventData> {
        return CashTransferEventData::class.java
    }

    override fun sendEvent(event: CashTransferEventData) {
        val transferOperation = event.transferOperation

        sendBalanceUpdateEvent(externalId = transferOperation.externalId,
                messageId =  event.messageId,
                clientBalanceUpdates = event.clientBalanceUpdates)
        sendCashTransferOperationEvent(transferOperation, event)
    }

    private fun sendCashTransferOperationEvent(transferOperation: TransferOperation, cashTransferEventData: CashTransferEventData) {
        val fee = if (CollectionUtils.isEmpty(transferOperation.fees)) null else transferOperation.fees!!.first()
        notificationQueue.put(CashTransferOperation(id = transferOperation.externalId,
                fromClientId = transferOperation.fromClientId,
                toClientId = transferOperation.toClientId,
                dateTime = transferOperation.dateTime,
                volume = NumberUtils.setScaleRoundHalfUp(transferOperation.volume, cashTransferEventData.transferOperation.asset!!.accuracy).toPlainString(),
                overdraftLimit = transferOperation.overdraftLimit,
                asset = transferOperation.asset!!.assetId,
                feeInstruction = fee,
                feeTransfer = singleFeeTransfer(fee, cashTransferEventData.fees),
                fees = cashTransferEventData.fees,
                messageId = cashTransferEventData.messageId))
    }

    private fun sendBalanceUpdateEvent(externalId: String,
                                       messageId: String,
                                       clientBalanceUpdates: List<ClientBalanceUpdate>) {
        oldFormatBalancesSender.sendBalanceUpdate(externalId,
                MessageType.CASH_TRANSFER_OPERATION,
                messageId,
                clientBalanceUpdates)
    }
}