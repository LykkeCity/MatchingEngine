package com.lykke.matching.engine.daos

import java.util.Date

class MarketOrder(id: String, uid: String, assetPairId: String, clientId: String, volume: Double,
                  var price: Double?, status: String, createdAt: Date, registered: Date,
                  var matchedAt: Date?, var straight: Boolean, var reservedLimitVolume: Double? = null, var dustSize: Double? = 0.0)
    : Order(id, uid, assetPairId, clientId, volume, status, createdAt, registered) {

    override fun isBuySide(): Boolean {
        return if (straight) super.isBuySide() else !super.isBuySide()
    }

    fun isOrigBuySide(): Boolean {
        return super.isBuySide()
    }
}