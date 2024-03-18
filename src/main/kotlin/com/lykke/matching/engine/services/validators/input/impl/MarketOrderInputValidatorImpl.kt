package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.MarketOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class MarketOrderInputValidatorImpl(private val applicationSettingsHolder: ApplicationSettingsHolder): MarketOrderInputValidator {
    override fun performValidation(marketOrderContext: MarketOrderContext) {
        val order = marketOrderContext.marketOrder

        val assetPair= marketOrderContext.assetPair
        isAssetKnown(order, assetPair)
        isAssetEnabled(order, assetPair!!)
        isVolumeValid(order, assetPair)
        isFeeValid(marketOrderContext.fee, marketOrderContext.fees, order)
        isVolumeAccuracyValid(order, marketOrderContext.baseAsset!!)
        isPriceAccuracyValid(order, assetPair)
    }

    private fun isFeeValid(feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?, order: MarketOrder) {
        if (!checkFee(feeInstruction, feeInstructions)) {
            throw OrderValidationException(OrderStatus.InvalidFee,
                    "Invalid fee (order id: ${order.id}, order externalId: ${order.externalId})")
        }
    }

    private fun isVolumeValid(order: MarketOrder, assetPair: AssetPair) {
        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, order.volume)) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "Volume can not be equal to zero")
        }

        if (!OrderValidationUtils.checkMinVolume(order, assetPair)) {
            throw OrderValidationException(OrderStatus.TooSmallVolume, "Too small volume for $order")
        }

        if (order.isStraight() && assetPair.maxVolume != null && order.getAbsVolume() > assetPair.maxVolume) {
            throw OrderValidationException(OrderStatus.InvalidVolume, "Too large volume for $order")
        }
        if (!order.isStraight() && assetPair.maxValue != null && order.getAbsVolume() > assetPair.maxValue) {
            throw OrderValidationException(OrderStatus.InvalidValue, "Too large value for $order")
        }
    }

    private fun isAssetEnabled(order: MarketOrder, assetPair: AssetPair) {
        if (applicationSettingsHolder.isAssetDisabled(assetPair.baseAssetId)
                || applicationSettingsHolder.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "Disabled asset $order")
        }
    }

    private fun isAssetKnown(order: MarketOrder, assetPair: AssetPair?) {
        if (assetPair == null) {
            throw OrderValidationException(OrderStatus.UnknownAsset,  "Unknown asset: ${order.assetPairId}")
        }
    }

    private fun isVolumeAccuracyValid(order: MarketOrder, baseAsset: Asset) {
        val baseAssetVolumeAccuracy = baseAsset.accuracy
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetVolumeAccuracy)

        if (!volumeAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy,
                    "Volume accuracy invalid form base assetId: $baseAssetVolumeAccuracy, volume: ${order.volume}")
        }
    }

    private fun isPriceAccuracyValid(order: MarketOrder, assetPair: AssetPair) {
        val price = order.price ?: return

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(price, assetPair.accuracy)

        if (!priceAccuracyValid) {
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy, "Invalid order accuracy, ${order.assetPairId}, price: ${order.price}")
        }
    }
}