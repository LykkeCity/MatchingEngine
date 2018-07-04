package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class BalanceUpdate(val walletId: String,
                    val assetId: String,
                    val oldBalance: String,
                    val newBalance: String,
                    val oldReserved: String,
                    val newReserved: String) : AbstractEventPart<OutgoingMessages.BalanceUpdate.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.BalanceUpdate.Builder {
        val builder = OutgoingMessages.BalanceUpdate.newBuilder()
        builder.setWalletId(walletId)
                .setAssetId(assetId)
                .setOldBalance(oldBalance)
                .setNewBalance(newBalance)
                .setOldReserved(oldReserved)
                .newReserved = newBalance
        return builder
    }
}