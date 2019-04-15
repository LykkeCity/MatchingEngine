package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.MarketOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class MarketOrderInputValidatorImpl(private val assetsPairsHolder: AssetsPairsHolder,
                                    private val assetsHolder: AssetsHolder,
                                    private val applicationSettingsHolder: ApplicationSettingsHolder): MarketOrderInputValidator {
    override fun performValidation(marketOrderContext: MarketOrderContext) {
        val order = marketOrderContext.marketOrder

        isAssetKnown(order)
        isAssetEnabled(order)
        isVolumeValid(order)
        isFeeValid(order)
        isVolumeAccuracyValid(order)
        isPriceAccuracyValid(order)
    }

    private fun isFeeValid(order: MarketOrder) {
        if (!checkFee(order.fee, order.fees)) {
            throw OrderValidationException(OrderStatus.InvalidFee,
                    "Invalid fee (order id: ${order.id}, order externalId: ${order.externalId})")
        }
    }

    private fun isVolumeValid(order: MarketOrder) {
        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, order.volume)) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "Volume can not be equal to zero")
        }

        if (!OrderValidationUtils.checkMinVolume(order, assetsPairsHolder.getAssetPair(order.assetPairId))) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "Too small volume for $order")
        }

        val assetPair = getAssetPair(order)
        if (order.isStraight() && assetPair.maxVolume != null && order.getAbsVolume() > assetPair.maxVolume) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "Too large volume for $order")
        }
        if (!order.isStraight() && assetPair.maxValue != null && order.getAbsVolume() > assetPair.maxValue) {
            throw OrderValidationException(OrderStatus.InvalidValue, "Too large value for $order")
        }
    }

    private fun isAssetEnabled(order: MarketOrder) {
        val assetPair = getAssetPair(order)
        if (applicationSettingsHolder.isAssetDisabled(assetPair.baseAssetId)
                || applicationSettingsHolder.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "Disabled asset $order")
        }
    }

    private fun isAssetKnown(order: MarketOrder) {
        try {
            getAssetPair(order)
        } catch (e: Exception) {
            throw OrderValidationException(OrderStatus.UnknownAsset,  "Unknown asset: ${order.assetPairId}")
        }
    }

    private fun isVolumeAccuracyValid(order: MarketOrder) {
        val baseAssetVolumeAccuracy = assetsHolder.getAsset(getBaseAsset(order)).accuracy
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetVolumeAccuracy)

        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy,
                    "Volume accuracy invalid form base assetId: $baseAssetVolumeAccuracy, volume: ${order.volume}")
        }
    }

    private fun isPriceAccuracyValid(order: MarketOrder) {
        val price = order.price ?: return

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(price, getAssetPair(order).accuracy)

        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "Invalid order accuracy, ${order.assetPairId}, price: ${order.price}")
        }
    }

    private fun getBaseAsset(order: MarketOrder): String {
        val assetPair = getAssetPair(order)
        return if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId
    }

    private fun getAssetPair(order: MarketOrder) = assetsPairsHolder.getAssetPair(order.assetPairId)
}