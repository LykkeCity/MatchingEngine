package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.util.Date

class NewLimitOrder(id: String,
                    externalId: String,
                    assetPairId: String,
                    clientId: String,
                    volume: Double,
                    var price: Double,
                    status: String,
                    statusDate: Date?,
                    createdAt: Date,
                    registered: Date,
                    var remainingVolume: Double,
                    var lastMatchTime: Date?,
                    reservedLimitVolume: Double? = null,
                    fee: LimitOrderFeeInstruction? = null,
                    fees: List<NewLimitOrderFeeInstruction>? = null,
                    @Version(3)
                    val type: LimitOrderType?,
                    @Version(3)
                    val lowerLimitPrice: Double?,
                    @Version(3)
                    val lowerPrice: Double?,
                    @Version(3)
                    val upperLimitPrice: Double?,
                    @Version(3)
                    val upperPrice: Double?,
                    @Transient
                    val previousExternalId: String?)
    : NewOrder(id, externalId, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees, statusDate), Serializable {

    fun getAbsRemainingVolume(): Double {
        return Math.abs(remainingVolume)
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

    override fun calculateReservedVolume(): Double {
        return if (isBuySide()) remainingVolume * price else getAbsRemainingVolume()
    }

    override fun updateMatchTime(time: Date) {
        lastMatchTime = time
    }

    override fun takePrice(): Double {
        return price
    }

    override fun updatePrice(price: Double) {
        //price is fixed
    }

    override fun updateRemainingVolume(volume: Double) {
        this.remainingVolume = volume
    }

    override fun copy(): NewLimitOrder {
        return NewLimitOrder(id, externalId, assetPairId, clientId, volume, price, status, statusDate, createdAt,
                registered, remainingVolume, lastMatchTime, reservedLimitVolume, fee as? LimitOrderFeeInstruction,
                fees?.map { it as NewLimitOrderFeeInstruction }, type, lowerLimitPrice, lowerPrice, upperLimitPrice,
                upperPrice, previousExternalId)
    }

    override fun applyToOrigin(origin: Copyable) {
        super.applyToOrigin(origin)
        origin as NewLimitOrder
        origin.remainingVolume = remainingVolume
        origin.lastMatchTime = lastMatchTime
    }
}