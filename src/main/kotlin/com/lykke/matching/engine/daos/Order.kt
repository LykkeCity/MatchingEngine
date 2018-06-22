package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.order.OrderStatus
import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

abstract class Order(
        val id: String,
        val externalId: String,
        val assetPairId: String,
        val clientId: String,
        val volume: BigDecimal,
        status: String,
        val createdAt: Date,
        val registered: Date,
        var reservedLimitVolume: BigDecimal?,
        val fee: FeeInstruction?,
        val fees: List<NewFeeInstruction>?,
        statusDate: Date?
) : Serializable, Copyable {

    var status = status
        private set

    var statusDate = statusDate
        private set

    fun getAbsVolume(): BigDecimal {
        return volume.abs()
    }

    open fun isBuySide(): Boolean {
        return NumberUtils.isPositive(volume)
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
        return minVolume == null || volume >= minVolume
    }

    fun checkVolume(assetsPairsHolder: AssetsPairsHolder): Boolean {
        return checkVolume(assetsPairsHolder.getAssetPair(assetPairId))
    }

    fun updateStatus(status: OrderStatus, date: Date) {
        if (status.name != this.status) {
            this.status = status.name
            this.statusDate = date
        }
    }

    override fun applyToOrigin(origin: Copyable) {
        origin as Order
        origin.status = status
        origin.reservedLimitVolume = reservedLimitVolume
    }
}