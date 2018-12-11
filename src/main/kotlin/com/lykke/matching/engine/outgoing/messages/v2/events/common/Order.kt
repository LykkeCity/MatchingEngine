package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderSide
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.outgoing.messages.v2.createProtobufTimestampBuilder
import java.util.Date

class Order(val orderType: OrderType,
            val id: String,
            val externalId: String,
            val assetPairId: String,
            val walletId: String,
            val side: OrderSide,
            val volume: String,
            val remainingVolume: String?,
            val price: String?,
            val status: OrderStatus,
            val rejectReason: OrderRejectReason?,
            val statusDate: Date,
            val createdAt: Date,
            val registered: Date,
            val lastMatchTime: Date?,
            val lowerLimitPrice: String?,
            val lowerPrice: String?,
            val upperLimitPrice: String?,
            val upperPrice: String?,
            val straight: Boolean?,
            val fees: List<FeeInstruction>?,
            val trades: List<Trade>?,
            val parentExternalId: String?,
            val childExternalId: String?) : EventPart<OutgoingMessages.ExecutionEvent.Order.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.ExecutionEvent.Order.Builder {
        val builder = OutgoingMessages.ExecutionEvent.Order.newBuilder()
        builder.setOrderType(orderType.id)
                .setId(id)
                .setExternalId(externalId)
                .setAssetPairId(assetPairId)
                .setWalletId(walletId)
                .setSide(side.id)
                .volume = volume
        remainingVolume?.let {
            builder.remainingVolume = it
        }
        price?.let {
            builder.price = it
        }
        builder.status = status.id
        rejectReason?.let {
            builder.rejectReason = rejectReason.name
        }
        builder.setStatusDate(statusDate.createProtobufTimestampBuilder())
                .setCreatedAt(createdAt.createProtobufTimestampBuilder())
                .setRegistered(registered.createProtobufTimestampBuilder())
        lastMatchTime?.let {
            builder.setLastMatchTime(it.createProtobufTimestampBuilder())
        }
        lowerLimitPrice?.let {
            builder.lowerLimitPrice = it
        }
        lowerPrice?.let {
            builder.lowerPrice = it
        }
        upperLimitPrice?.let {
            builder.upperLimitPrice = it
        }
        upperPrice?.let {
            builder.upperPrice = it
        }
        straight?.let {
            builder.straight = it
        }
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        trades?.forEach { trade ->
            builder.addTrades(trade.createGeneratedMessageBuilder())
        }
        parentExternalId?.let {
            builder.parentExternalId = it
        }
        childExternalId?.let {
            builder.childExternalId = it
        }
        return builder
    }

}