package com.lykke.matching.engine.daos

import java.io.Serializable
import java.util.Date

class NewLimitOrder(id: String, externalId: String, assetPairId: String, clientId: String, volume: Double, var price: Double,
                    status: String, createdAt: Date, registered: Date, var remainingVolume: Double, var lastMatchTime: Date?,
                    reservedLimitVolume: Double? = null, fee: FeeInstruction? = null)
    : NewOrder(id, externalId, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee), Serializable {

    fun getAbsRemainingVolume(): Double {
        return Math.abs(remainingVolume)
    }

    override fun isOrigBuySide(): Boolean {
        return super.isBuySide()
    }

    override fun isStraight(): Boolean {
        return true
    }

    override fun calculateReservedVolume(): Double {
        return if (isBuySide()) remainingVolume * price else remainingVolume
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
}