package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

class LimitOrder(id: String, externalId: String, assetPairId: String, clientId: String, volume: BigDecimal, var price: BigDecimal,
                 status: String, createdAt: Date, registered: Date, var remainingVolume: BigDecimal, var lastMatchTime: Date?,
                 reservedLimitVolume: BigDecimal? = null, fee: LimitOrderFeeInstruction? = null, fees: List<NewLimitOrderFeeInstruction>? = null,
                 @Version(3)
                 val type: LimitOrderType?,
                 @Version(3)
                 val lowerLimitPrice: BigDecimal?,
                 @Version(3)
                 val lowerPrice: BigDecimal?,
                 @Version(3)
                 val upperLimitPrice: BigDecimal?,
                 @Version(3)
                 val upperPrice: BigDecimal?,
                 @Transient
                 val previousExternalId: String?)
    : Order(id, externalId, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees), Serializable {

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

    override fun copy(): Order {
        return LimitOrder(id, externalId, assetPairId, clientId, volume, price, status, createdAt, registered,
                remainingVolume, lastMatchTime, reservedLimitVolume, fee as? LimitOrderFeeInstruction, fees?.map { it as NewLimitOrderFeeInstruction }, type, lowerLimitPrice, lowerPrice, upperLimitPrice, upperPrice, previousExternalId)
    }

    override fun applyToOrigin(origin: Copyable) {
        super.applyToOrigin(origin)
        origin as LimitOrder
        origin.remainingVolume = remainingVolume
        origin.lastMatchTime = lastMatchTime
    }
}