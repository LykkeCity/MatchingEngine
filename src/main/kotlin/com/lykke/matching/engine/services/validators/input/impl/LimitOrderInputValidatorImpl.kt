package com.lykke.matching.engine.services.validators.input.impl


import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class LimitOrderInputValidatorImpl(private val applicationSettingsCache: ApplicationSettingsCache,
                                   private val assetsPairHolder: AssetsPairsHolder) : LimitOrderInputValidator {
    override fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        if (!singleLimitContext.isTrustedClient) {
            validateFee(singleLimitContext)
            validateAssets(singleLimitContext)
        }

        validatePrice(singleLimitContext)
        validateVolume(singleLimitContext)
        validatePriceAccuracy(singleLimitContext)
        validateVolumeAccuracy(singleLimitContext)
    }

    override fun checkVolume(order: Order): Boolean {
        val assetPair = assetsPairHolder.getAssetPair(order.assetPairId)
        val volume = order.getAbsVolume()
        val minVolume = if (order.isStraight()) assetPair.minVolume else assetPair.minInvertedVolume
        return minVolume == null || volume >= minVolume
    }

    override fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        validateFee(singleLimitContext)
        validateAssets(singleLimitContext)
        validateLimitPrices(singleLimitContext)
        validateVolume(singleLimitContext)
        validateVolumeAccuracy(singleLimitContext)
        validatePriceAccuracy(singleLimitContext)
    }

    fun validateFee(singleLimitContext: SingleLimitContext) {
        val order = singleLimitContext.limitOrder
        if (order.fee != null && order.fees?.size ?: 0 > 1 || !checkFee(null, order.fees)) {
            throw OrderValidationException(OrderStatus.InvalidFee, "has invalid fee")
        }
    }

    fun validateAssets(singleLimitContext: SingleLimitContext) {
        val assetPair = singleLimitContext.assetPair
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "disabled asset")
        }
    }

    fun validatePrice(singleLimitContext: SingleLimitContext) {
        if (singleLimitContext.limitOrder.price <= BigDecimal.ZERO) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "price is invalid")
        }
    }

    fun validateLimitPrices(singleLimitContext: SingleLimitContext) {
        val order = singleLimitContext.limitOrder
        if (((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) ||
                ((order.upperLimitPrice == null).xor(order.upperPrice == null)) ||
                (order.lowerLimitPrice != null && (order.lowerLimitPrice <= BigDecimal.ZERO || order.lowerPrice!! <= BigDecimal.ZERO)) ||
                (order.upperLimitPrice != null && (order.upperLimitPrice <= BigDecimal.ZERO || order.upperPrice!! <= BigDecimal.ZERO)) ||
                (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice)) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "limit prices are invalid")
        }
    }

    fun validateVolume(singleLimitContext: SingleLimitContext) {
        if (!checkVolume(singleLimitContext.limitOrder)) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "volume is too small")
        }
    }

    fun validateVolumeAccuracy(singleLimitContext: SingleLimitContext) {
        val baseAssetAccuracy = singleLimitContext.baseAsset.accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(singleLimitContext.limitOrder.volume, baseAssetAccuracy)
        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy, "volume accuracy is invalid")
        }
    }

    fun validatePriceAccuracy(singleLimitContext: SingleLimitContext) {
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(singleLimitContext.limitOrder.price, singleLimitContext.assetPair.accuracy)
        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "price accuracy is invalid")
        }
    }
}