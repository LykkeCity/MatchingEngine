package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.utils.NumberUtils
import java.util.Date

class MarketOrder(id: String,
                  uid: String,
                  assetPairId: String,
                  clientId: String,
                  volume: Double,
                  var price: Double?,
                  status: String,
                  statusDate: Date,
                  createdAt: Date,
                  registered: Date,
                  var matchedAt: Date?,
                  val straight: Boolean,
                  reservedLimitVolume: Double? = null,
                  fee: FeeInstruction? = null,
                  fees: List<NewFeeInstruction>? = null)
    : NewOrder(id, uid, assetPairId, clientId, volume, status, createdAt, registered, reservedLimitVolume, fee, fees, statusDate) {

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

    override fun copy(): MarketOrder {
        return MarketOrder(id, externalId, assetPairId, clientId, volume, price, status, statusDate!!, createdAt, registered, matchedAt, straight, reservedLimitVolume, fee, fees)
    }

    override fun applyToOrigin(origin: Copyable) {
        super.applyToOrigin(origin)
        origin as MarketOrder
        origin.price = price
        origin.matchedAt = matchedAt
    }

    override fun toString(): String {
        return "market order id: $id, client: $clientId, asset: $assetPairId, " +
                "volume: ${NumberUtils.roundForPrint(volume)}, straight: $straight"
    }
}