package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import java.util.Date

class MarketOrder(id: String, uid: String, assetPairId: String, clientId: String, volume: Double,
                  var price: Double?, status: String, createdAt: Date, registered: Date,
                  var matchedAt: Date?, var straight: Boolean, reservedLimitVolume: Double? = null, fee: FeeInstruction? = null, fees: List<NewFeeInstruction>? = null)
    : NewOrder(id, uid, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees) {

    override fun isOrigBuySide(): Boolean {
        return super.isBuySide()
    }

    override fun isBuySide(): Boolean {
        return if (straight) super.isBuySide() else !super.isBuySide()
    }

    override fun isStraight(): Boolean {
        return straight
    }

    override fun calculateReservedVolume(): Double {
        return reservedLimitVolume ?: 0.0
    }

    override fun updateMatchTime(time: Date) {
        matchedAt = time
    }

    override fun takePrice(): Double? {
        return price
    }

    override fun updatePrice(price: Double) {
        this.price = price
    }

    override fun updateRemainingVolume(volume: Double) {
        //nothing to do
    }
}