package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class CashOut(val walletId: String,
              val assetId: String,
              val volume: String,
              val fees: List<Fee>?) : EventPart<OutgoingMessages.CashOutEvent.CashOut.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.CashOutEvent.CashOut.Builder {
        val builder = OutgoingMessages.CashOutEvent.CashOut.newBuilder()
        builder.setWalletId(walletId)
                .setAssetId(assetId)
                .volume = volume
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }

}