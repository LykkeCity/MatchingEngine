package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.OrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderInputValidatorImpl(val applicationSettingsHolder: ApplicationSettingsHolder): OrderInputValidator {
    override fun validateAsset(assetPair: AssetPair?, assetPairId: String) {
        if (assetPair == null) {
            throw OrderValidationException(OrderStatus.UnknownAsset, "Unable to find asset pair $assetPairId")
        }

        if (applicationSettingsHolder.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsHolder.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "disabled asset")
        }
    }

    override fun validateFee(order: LimitOrder) {
        if (order.fee != null && order.fees?.size ?: 0 > 1 || !checkFee(null, order.fees)) {
            throw OrderValidationException(OrderStatus.InvalidFee, "has invalid fee")
        }
    }

    override fun validatePrice(limitOrder: LimitOrder) {
        if (limitOrder.price <= BigDecimal.ZERO) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "price is invalid")
        }
    }

    override fun validateLimitPrices(order: LimitOrder) {
        if ((order.lowerLimitPrice == null && order.lowerPrice == null && order.upperLimitPrice == null && order.upperPrice == null) ||
                ((order.lowerLimitPrice == null).xor(order.lowerPrice == null)) ||
                ((order.upperLimitPrice == null).xor(order.upperPrice == null)) ||
                (order.lowerLimitPrice != null && (order.lowerLimitPrice <= BigDecimal.ZERO || order.lowerPrice!! <= BigDecimal.ZERO)) ||
                (order.upperLimitPrice != null && (order.upperLimitPrice <= BigDecimal.ZERO || order.upperPrice!! <= BigDecimal.ZERO)) ||
                (order.lowerLimitPrice != null && order.upperLimitPrice != null && order.lowerLimitPrice >= order.upperLimitPrice)) {
            throw OrderValidationException(OrderStatus.InvalidPrice, "limit prices are invalid")
        }
    }

    override fun validateValue(order: LimitOrder, assetPair: AssetPair) {
        if (assetPair.maxValue != null && order.getAbsVolume() * order.price > assetPair.maxValue) {
            throw OrderValidationException(OrderStatus.InvalidValue, "value is too large")
        }
    }

    override fun validateMaxValue(limitOrder: LimitOrder, assetPair: AssetPair) {
        if (assetPair.maxVolume != null && limitOrder.getAbsVolume() > assetPair.maxVolume) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "volume is too large")
        }
    }

    override fun validateStopOrderMaxValue(limitOrder: LimitOrder, assetPair: AssetPair) {
        validateMaxValue(limitOrder, assetPair)

        if (assetPair.maxValue != null && (limitOrder.lowerLimitPrice != null && limitOrder.getAbsVolume() * limitOrder.lowerPrice!! > assetPair.maxValue
                        || limitOrder.upperLimitPrice != null && limitOrder.getAbsVolume() * limitOrder.upperPrice!! > assetPair.maxValue)) {
            throw OrderValidationException(OrderStatus.InvalidValue, "value is too large")
        }
    }

    override fun validateVolume(limitOrder: LimitOrder, assetPair: AssetPair) {

        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, limitOrder.volume)) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "volume can not be equal to zero")
        }

        if (!OrderValidationUtils.checkMinVolume(limitOrder, assetPair)) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "volume is too small")
        }
    }

    override fun validateVolumeAccuracy(limitOrder: LimitOrder, baseAsset: Asset) {
        val baseAssetAccuracy = baseAsset.accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(limitOrder.volume, baseAssetAccuracy)
        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy, "volume accuracy is invalid")
        }
    }

    override fun validatePriceAccuracy(limitOrder: LimitOrder, assetPair: AssetPair) {
        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(limitOrder.price, assetPair.accuracy)
        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "price accuracy is invalid")
        }
    }
}