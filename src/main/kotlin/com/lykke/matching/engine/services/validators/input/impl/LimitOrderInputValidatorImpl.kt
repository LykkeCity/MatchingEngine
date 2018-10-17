package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderFatalValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class LimitOrderInputValidatorImpl(val applicationSettingsCache: ApplicationSettingsCache) : LimitOrderInputValidator {
    override fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitOrderContext

        validateLimitOrder(singleLimitContext.isTrustedClient,
                singleLimitContext.limitOrder,
                singleLimitContext.assetPair,
                singleLimitOrderParsedData.inputAssetPairId,
                singleLimitContext.baseAsset)
    }

    override fun validateLimitOrder(isTrustedClient: Boolean,
                                    order: LimitOrder,
                                    assetPair: AssetPair?,
                                    assetPairId: String,
                                    baseAsset: Asset?) {
        if (!isTrustedClient) {
            validateFee(order)
        }

        validateAsset(assetPair, assetPairId)
        validatePrice(order)
        validateVolume(order, assetPair!!)
        validateMaxValue(order, assetPair)
        validatePriceAccuracy(order, assetPair)
        validateVolumeAccuracy(order, baseAsset!!)
        validateValue(order, assetPair)
    }

    override fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData) {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitOrderContext
        val limitOrder = singleLimitContext.limitOrder
        val assetPair = singleLimitContext.assetPair

        validateAsset(assetPair, singleLimitOrderParsedData.inputAssetPairId)
        validateFee(limitOrder)
        validateLimitPrices(limitOrder)
        validateVolume(limitOrder, assetPair!!)
        validateStopOrderMaxValue(limitOrder, assetPair)
        validateVolumeAccuracy(limitOrder, singleLimitContext.baseAsset!!)
        validateStopPricesAccuracy(limitOrder, assetPair)
    }

    private fun validateAsset(assetPair: AssetPair?, assetPairId: String) {
        if (assetPair == null) {
            throw OrderFatalValidationException("Unable to find asset pair $assetPairId")
        }

        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "disabled asset")
        }
    }

    private fun validateFee(order: LimitOrder) {
        if (order.fee != null && order.fees?.size ?: 0 > 1 || !checkFee(null, order.fees)) {
            throw OrderValidationException(OrderStatus.InvalidFee, "has invalid fee")
        }
    }

    private fun validatePrice(limitOrder: LimitOrder) {
        if (limitOrder.price <= BigDecimal.ZERO) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "price is invalid")
        }
    }

    private fun validateLimitPrices(order: LimitOrder) {
        if ((order.lowerLimitPrice == null && order.lowerPrice == null && order.upperLimitPrice == null && order.upperPrice == null) ||
                ((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) ||
                ((order.upperLimitPrice == null).xor(order.upperPrice == null)) ||
                (order.lowerLimitPrice != null && (order.lowerLimitPrice <= BigDecimal.ZERO || order.lowerPrice!! <= BigDecimal.ZERO)) ||
                (order.upperLimitPrice != null && (order.upperLimitPrice <= BigDecimal.ZERO || order.upperPrice!! <= BigDecimal.ZERO)) ||
                (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice)) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "limit prices are invalid")
        }
    }

    private fun validateValue(order: LimitOrder, assetPair: AssetPair) {
        if (assetPair.maxValue != null && order.getAbsVolume() * order.price > assetPair.maxValue) {
            throw OrderValidationException(OrderStatus.InvalidValue, "value is too large")
        }
    }

    private fun validateMaxValue(limitOrder: LimitOrder, assetPair: AssetPair) {
        if (assetPair.maxVolume != null && limitOrder.getAbsVolume() > assetPair.maxVolume) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "volume is too large")
        }
    }

    private fun validateStopOrderMaxValue(limitOrder: LimitOrder, assetPair: AssetPair) {
        validateMaxValue(limitOrder, assetPair)

        if (assetPair.maxValue != null && (limitOrder.lowerLimitPrice != null && limitOrder.getAbsVolume() * limitOrder.lowerPrice!! > assetPair.maxValue
                        || limitOrder.upperLimitPrice != null && limitOrder.getAbsVolume() * limitOrder.upperPrice!! > assetPair.maxValue)) {
            throw OrderValidationException(OrderStatus.InvalidValue, "value is too large")
        }
    }

    private fun validateVolume(limitOrder: LimitOrder, assetPair: AssetPair) {

        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, limitOrder.volume)) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "volume can not be equal to zero")
        }

        if (!OrderValidationUtils.checkMinVolume(limitOrder, assetPair)) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "volume is too small")
        }
    }

    private fun validateVolumeAccuracy(limitOrder: LimitOrder, baseAsset: Asset) {
        val baseAssetAccuracy = baseAsset.accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(limitOrder.volume, baseAssetAccuracy)
        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy, "volume accuracy is invalid")
        }
    }

    private fun validatePriceAccuracy(limitOrder: LimitOrder, assetPair: AssetPair) {
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(limitOrder.price, assetPair.accuracy)
        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "price accuracy is invalid")
        }
    }

    private fun validateStopPricesAccuracy(order: LimitOrder, assetPair: AssetPair) {
        val priceAccuracy = assetPair.accuracy
        if (order.lowerLimitPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.lowerLimitPrice, priceAccuracy)
                || order.lowerPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.lowerPrice, priceAccuracy)
                || order.upperLimitPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.upperLimitPrice, priceAccuracy)
                || order.upperPrice != null && !NumberUtils.isScaleSmallerOrEqual(order.upperPrice, priceAccuracy)) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "stop order price accuracy is invalid")
        }
    }
}