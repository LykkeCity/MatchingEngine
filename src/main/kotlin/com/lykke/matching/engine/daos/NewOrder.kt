package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.RoundingUtils
import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

abstract class NewOrder(
        val id: String,
        val externalId: String,
        val assetPairId: String,
        val clientId: String,
        val volume: BigDecimal,
        var status: String,
        val createdAt: Date,
        val registered: Date,
        var reservedLimitVolume: BigDecimal?,
        @Version (1) // for compatibility with old serialized orders
        val fee: FeeInstruction?,
        @Version (2)
        val fees: List<NewFeeInstruction>?
) : Serializable, Copyable {

    fun getAbsVolume(): BigDecimal {
        return volume.abs()
    }

    open fun isBuySide(): Boolean {
        return RoundingUtils.isPositive(volume)
    }

    abstract fun isOrigBuySide(): Boolean
    abstract fun isStraight(): Boolean
    abstract fun calculateReservedVolume(): BigDecimal
    abstract fun updateMatchTime(time: Date)
    abstract fun takePrice(): BigDecimal?
    abstract fun updatePrice(price: BigDecimal)
    abstract fun updateRemainingVolume(volume: BigDecimal)

    fun checkVolume(assetPair: AssetPair): Boolean {
        val volume = getAbsVolume()
        val minVolume = if (isStraight()) assetPair.minVolume else assetPair.minInvertedVolume
        val volumeToMinVolumeComparison = volume.compareTo(minVolume)
        return minVolume == null || volumeToMinVolumeComparison >= 1
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