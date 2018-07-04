package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class CashIn(val walletId: String,
             val assetId: String,
             val volume: String,
             val fees: List<Fee>?) : AbstractEventPart<OutgoingMessages.CashInEvent.CashIn.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.CashInEvent.CashIn.Builder {
        val builder = OutgoingMessages.CashInEvent.CashIn.newBuilder()
        builder.setWalletId(walletId)
                .setAssetId(assetId)
                .volume = volume
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }

}