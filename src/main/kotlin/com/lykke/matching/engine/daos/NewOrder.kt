package com.lykke.matching.engine.daos

import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.util.Date

abstract class NewOrder(
        var id: String,
        var externalId: String,
        var assetPairId: String,
        var clientId: String,
        var volume: Double,
        var status: String,
        var createdAt: Date,
        var registered: Date,
        var reservedLimitVolume: Double?,
        @Version (1) // for compatibility with old serialized orders
        var fee: FeeInstruction?
) : Serializable {

    fun getAbsVolume(): Double {
        return Math.abs(volume)
    }

    open fun isBuySide(): Boolean {
        return volume > 0
    }

    abstract fun isOrigBuySide(): Boolean
    abstract fun isStraight(): Boolean
    abstract fun calculateReservedVolume(): Double
    abstract fun updateMatchTime(time: Date)
    abstract fun takePrice(): Double?
    abstract fun updatePrice(price: Double)
    abstract fun updateRemainingVolume(volume: Double)
}