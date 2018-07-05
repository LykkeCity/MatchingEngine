package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class FeeTransfer(val volume: String,
                  val sourceWalletId: String,
                  val targetWalletId: String,
                  val assetId: String,
                  val feeCoef: String?,
                  val index: Int) : EventPart<OutgoingMessages.FeeTransfer.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.FeeTransfer.Builder {
        val builder = OutgoingMessages.FeeTransfer.newBuilder()
        builder.setVolume(volume)
                .setSourceWalletId(sourceWalletId)
                .setTargetWalletId(targetWalletId)
                .assetId = assetId
        feeCoef?.let {
            builder.feeCoef = it
        }
        builder.index = index
        return builder
    }

}