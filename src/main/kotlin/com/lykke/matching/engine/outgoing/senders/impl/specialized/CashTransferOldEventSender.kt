package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
@Deprecated("Old format of outgoing message is deprecated")
class CashTransferOldEventSender(val notificationQueue: BlockingQueue<CashTransferOperation>) : SpecializedEventSender {
    override fun getProcessedMessageClass(): Class<*> {
        return CashTransferEventData::class.java
    }

    override fun sendEvent(outgoingEventData: OutgoingEventData) {
        val cashTransferEventData = outgoingEventData.eventData as CashTransferEventData
        val transferOperation = cashTransferEventData.transferOperation

        sendBalanceUpdateEvent(cashTransferEventData, transferOperation)
        sendCashTransferOperationEvent(transferOperation, cashTransferEventData)
    }

    private fun sendCashTransferOperationEvent(transferOperation: TransferOperation, cashTransferEventData: CashTransferEventData) {
        val fee = if(transferOperation.fees == null || transferOperation.fees.isEmpty()) null else transferOperation.fees.first()
        notificationQueue.put(CashTransferOperation(transferOperation.externalId,
                transferOperation.fromClientId,
                transferOperation.toClientId,
                transferOperation.dateTime,
                NumberUtils.setScaleRoundHalfUp(transferOperation.volume, cashTransferEventData.transferOperation.asset!!.accuracy).toPlainString(),
                transferOperation.overdraftLimit,
                transferOperation.asset!!.assetId,
                fee,
                singleFeeTransfer(fee, cashTransferEventData.fees),
                cashTransferEventData.fees,
                cashTransferEventData.messageId))
    }

    private fun sendBalanceUpdateEvent(cashTransferEventData: CashTransferEventData, transferOperation: TransferOperation) {
        cashTransferEventData.walletProcessor.sendNotification(transferOperation.externalId, MessageType.CASH_TRANSFER_OPERATION.name, cashTransferEventData.messageId)
    }
}