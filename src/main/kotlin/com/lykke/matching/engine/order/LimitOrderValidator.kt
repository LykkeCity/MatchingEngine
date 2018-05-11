package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class LimitOrderValidator(private val assetsPairsHolder: AssetsPairsHolder,
                          private val assetsHolder: AssetsHolder,
                          private val applicationSettingsCache: ApplicationSettingsCache) {

    fun validateFee(order: NewLimitOrder) {
        if (order.fee != null && order.fees?.size ?: 0 > 1 || !checkFee(null, order.fees)) {
            throw OrderValidationException("has invalid fee", OrderStatus.InvalidFee)
        }
    }

    fun validateAssets(assetPair: AssetPair) {
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException("disabled asset", OrderStatus.DisabledAsset)
        }
    }

    fun validatePrice(order: NewLimitOrder) {
        if (order.price <= 0.0) {
            throw OrderValidationException("price is invalid", OrderStatus.InvalidPrice)
        }
    }

    fun validateLimitPrices(order: NewLimitOrder) {
        var checked = false
        try {
            if ((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) return
            if ((order.upperLimitPrice == null).xor(order.upperPrice == null)) return
            if (order.lowerLimitPrice != null && (order.lowerLimitPrice <= 0.0 || order.lowerPrice!! <= 0.0)) return
            if (order.upperLimitPrice != null && (order.upperLimitPrice <= 0.0 || order.upperPrice!! <= 0.0)) return
            if (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice) return
            checked = true
        } finally {
            if (!checked) throw OrderValidationException("limit prices are invalid", OrderStatus.InvalidPrice)
        }
    }

    fun validateVolume(order: NewLimitOrder) {
        if (!order.checkVolume(assetsPairsHolder)) {
            throw OrderValidationException("volume is too small", OrderStatus.TooSmallVolume)
        }
    }

    fun validateVolumeAccuracy(order: NewLimitOrder) {
        val baseAssetAccuracy = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(order.assetPairId).baseAssetId).accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetAccuracy)
        if (!volumeAccuracyValid) {
            throw OrderValidationException("volume accuracy is invalid", OrderStatus.InvalidVolumeAccuracy)
        }
    }

    fun validatePriceAccuracy(order: NewLimitOrder) {
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.price, assetsPairsHolder.getAssetPair(order.assetPairId).accuracy)
        if (!priceAccuracyValid) {
            throw OrderValidationException("price accuracy is invalid", OrderStatus.InvalidPriceAccuracy)
        }
    }

    fun checkBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException("not enough funds to reserve", OrderStatus.NotEnoughFunds)
        }
    }
}