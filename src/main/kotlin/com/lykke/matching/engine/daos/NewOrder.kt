package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.holders.AssetsPairsHolder
import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.util.Date

abstract class NewOrder(
        val id: String,
        val externalId: String,
        val assetPairId: String,
        val clientId: String,
        val volume: Double,
        var status: String,
        val createdAt: Date,
        val registered: Date,
        var reservedLimitVolume: Double?,
        @Version (1) // for compatibility with old serialized orders
        val fee: FeeInstruction?,
        @Version (2)
        val fees: List<NewFeeInstruction>?
) : Serializable, Copyable {

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

    fun checkVolume(assetPair: AssetPair): Boolean {
        val volume = getAbsVolume()
        val minVolume = if (isStraight()) assetPair.minVolume else assetPair.minInvertedVolume
        return minVolume == null || volume >= minVolume.toDouble()
    }

    fun checkVolume(assetsPairsHolder: AssetsPairsHolder): Boolean {
        return checkVolume(assetsPairsHolder.getAssetPair(assetPairId))
    }

    override fun applyToOrigin(origin: Copyable) {
        origin as NewOrder
        origin.status = status
        origin.reservedLimitVolume = reservedLimitVolume
    }
}