package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

class LimitOrder(id: String,
                 externalId: String,
                 assetPairId: String,
                 clientId: String,
                 volume: BigDecimal,
                 var price: BigDecimal,
                 status: String,
                 statusDate: Date?,
                 createdAt: Date,
                 registered: Date?,
                 var remainingVolume: BigDecimal,
                 var lastMatchTime: Date?,
                 reservedLimitVolume: BigDecimal? = null,
                 fee: LimitOrderFeeInstruction? = null,
                 fees: List<NewLimitOrderFeeInstruction>? = null,
                 val type: LimitOrderType?,
                 val lowerLimitPrice: BigDecimal?,
                 val lowerPrice: BigDecimal?,
                 val upperLimitPrice: BigDecimal?,
                 val upperPrice: BigDecimal?,
                 @Transient
                 val previousExternalId: String?)
    : Order(id, externalId, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees, statusDate), Serializable {

    constructor(limitOrder: LimitOrder, statusDate: Date, registered: Date): this(
            limitOrder.id,
            limitOrder.externalId,
            limitOrder.assetPairId,
            limitOrder.clientId,
            limitOrder.volume,
            limitOrder.price,
            limitOrder.status,
            statusDate,
            limitOrder.createdAt,
            registered,
            limitOrder.remainingVolume,
            limitOrder.lastMatchTime,
            limitOrder.reservedLimitVolume,
            limitOrder.fee as? LimitOrderFeeInstruction,
            limitOrder.fees?.map { it as NewLimitOrderFeeInstruction },
            limitOrder.type,
            limitOrder.lowerLimitPrice,
            limitOrder.lowerPrice,
            limitOrder.upperLimitPrice,
            limitOrder.upperPrice,
            limitOrder.previousExternalId)

    fun getAbsRemainingVolume(): BigDecimal {
        return remainingVolume.abs()
    }

    fun isPartiallyMatched(): Boolean {
        return remainingVolume != volume
    }

    override fun isOrigBuySide(): Boolean {
        return super.isBuySide()
    }

    override fun isStraight(): Boolean {
        return true
    }

    override fun calculateReservedVolume(): BigDecimal {
        return if (isBuySide()) remainingVolume * price else getAbsRemainingVolume()
    }

    override fun updateMatchTime(time: Date) {
        lastMatchTime = time
    }

    override fun takePrice(): BigDecimal {
        return price
    }

    override fun updatePrice(price: BigDecimal) {
        //price is fixed
    }

    override fun updateRemainingVolume(volume: BigDecimal) {
        this.remainingVolume = volume
    }

    override fun copy(): LimitOrder {
        return LimitOrder(id, externalId, assetPairId, clientId, volume, price, status, statusDate, createdAt,
                registered, remainingVolume, lastMatchTime, reservedLimitVolume, fee as? LimitOrderFeeInstruction,
                fees?.map { it as NewLimitOrderFeeInstruction }, type, lowerLimitPrice, lowerPrice, upperLimitPrice,
                upperPrice, previousExternalId)
    }

    override fun applyToOrigin(origin: Copyable) {
        super.applyToOrigin(origin)
        origin as LimitOrder
        origin.remainingVolume = remainingVolume
        origin.lastMatchTime = lastMatchTime
    }
}