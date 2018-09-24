package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class LimitOrderValidator(private val assetsPairsHolder: AssetsPairsHolder,
                          private val assetsHolder: AssetsHolder,
                          private val applicationSettingsCache: ApplicationSettingsCache) {

    fun validateFee(order: LimitOrder) {
        if (order.fee != null && order.fees?.size ?: 0 > 1 || !checkFee(null, order.fees)) {
            throw OrderValidationException(OrderStatus.InvalidFee, "has invalid fee")
        }
    }

    fun validateAssets(assetPair: AssetPair) {
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "disabled asset")
        }
    }

    fun validatePrice(order: LimitOrder) {
        if (order.price <= BigDecimal.ZERO) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "price is invalid")
        }
    }

    fun validateLimitPrices(order: LimitOrder) {
        var checked = false
        try {
            if (order.lowerLimitPrice == null && order.lowerPrice == null && order.upperLimitPrice == null && order.upperPrice == null) return
            if ((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) return
            if ((order.upperLimitPrice == null).xor(order.upperPrice == null)) return
            if (order.lowerLimitPrice != null && (order.lowerLimitPrice <= BigDecimal.ZERO || order.lowerPrice!! <= BigDecimal.ZERO)) return
            if (order.upperLimitPrice != null && (order.upperLimitPrice <= BigDecimal.ZERO || order.upperPrice!! <= BigDecimal.ZERO)) return
            if (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice) return
            checked = true
        } finally {
            if (!checked) throw OrderValidationException(OrderStatus.InvalidPrice, "limit prices are invalid")
        }
    }

    fun validateVolume(order: LimitOrder, assetPair: AssetPair) {
        if (!order.checkMinVolume(assetPair)) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "volume is too small")
        }
        if (assetPair.maxVolume != null && order.getAbsVolume() > assetPair.maxVolume) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "volume is too large")
        }
        if (assetPair.maxValue != null && order.getAbsVolume() * order.price > assetPair.maxValue) {
            throw OrderValidationException(OrderStatus.InvalidValue, "value is too large")
        }
    }

    fun validateVolumeAccuracy(order: LimitOrder) {
        val baseAssetAccuracy = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(order.assetPairId).baseAssetId).accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetAccuracy)
        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy, "volume accuracy is invalid")
        }
    }

    fun validatePriceAccuracy(order: LimitOrder) {
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.price, assetsPairsHolder.getAssetPair(order.assetPairId).accuracy)
        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "price accuracy is invalid")
        }
    }

    fun validateStopPricesAccuracy(order: LimitOrder) {
        val priceAccuracy = assetsPairsHolder.getAssetPair(order.assetPairId).accuracy
        if (order.lowerLimitPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.lowerLimitPrice, priceAccuracy)
        || order.lowerPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.lowerPrice, priceAccuracy)
        || order.upperLimitPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.upperLimitPrice, priceAccuracy)
        || order.upperPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.upperPrice, priceAccuracy)) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "stop order price accuracy is invalid")
        }
    }

    fun checkBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "not enough funds to reserve")
        }
    }
}