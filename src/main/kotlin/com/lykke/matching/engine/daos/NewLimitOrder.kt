package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.updater.Copyable
import java.io.Serializable
import java.util.Date

class NewLimitOrder(id: String, externalId: String, assetPairId: String, clientId: String, volume: Double, val price: Double,
                    status: String, createdAt: Date, registered: Date, var remainingVolume: Double, var lastMatchTime: Date?,
                    reservedLimitVolume: Double? = null, fee: LimitOrderFeeInstruction? = null, fees: List<NewLimitOrderFeeInstruction>? = null)
    : NewOrder(id, externalId, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees), Serializable {

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
        return NewLimitOrder(id, externalId, assetPairId, clientId, volume, price, status, createdAt, registered,
                remainingVolume, lastMatchTime, reservedLimitVolume, fee as? LimitOrderFeeInstruction, fees?.map { it as NewLimitOrderFeeInstruction })
    }

    override fun applyToOrigin(origin: Copyable) {
        super.applyToOrigin(origin)
        origin as NewLimitOrder
        origin.remainingVolume = remainingVolume
        origin.lastMatchTime = lastMatchTime
    }
}