package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import java.util.Date

class Trade(val tradeId: String,
            val assetId: String,
            val volume: String,
            val price: String,
            val timestamp: Date,
            val oppositeOrderId: String,
            val oppositeExternalOrderId: String,
            val oppositeWalletId: String,
            val oppositeAssetId: String,
            val oppositeVolume: String,
            val index: Int,
            val absoluteSpread: String?,
            val relativeSpread: String?,
            val role: TradeRole,
            val fees: List<FeeTransfer>?) : AbstractEventPart<OutgoingMessages.ExecutionEvent.Order.Trade.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.ExecutionEvent.Order.Trade.Builder {
        val builder = OutgoingMessages.ExecutionEvent.Order.Trade.newBuilder()
        builder.setTradeId(tradeId)
                .setAssetId(assetId)
                .setVolume(volume)
                .setPrice(price)
                .setTimestamp(timestamp.createProtobufTimestampBuilder())
                .setOppositeOrderId(oppositeOrderId)
                .setOppositeExternalOrderId(oppositeExternalOrderId)
                .setOppositeWalletId(oppositeWalletId)
                .setOppositeAssetId(oppositeAssetId)
                .setOppositeVolume(oppositeVolume)
                .setIndex(index)
                .role = role.id
        absoluteSpread?.let {
            builder.setAbsoluteSpread(it)
        }
        relativeSpread?.let {
            builder.setRelativeSpread(it)
        }
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }

}