package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class CashTransfer(val fromWalletId: String,
                   val toWalletId: String,
                   val volume: String,
                   val overdraftLimit: String?,
                   val assetId: String,
                   val fees: List<Fee>?) : EventPart<OutgoingMessages.CashTransferEvent.CashTransfer.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.CashTransferEvent.CashTransfer.Builder {
        val builder = OutgoingMessages.CashTransferEvent.CashTransfer.newBuilder()
        builder.setFromWalletId(fromWalletId)
                .setToWalletId(toWalletId)
                .setVolume(volume)
                .setOverdraftLimit(overdraftLimit)
                .assetId = assetId
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }

}