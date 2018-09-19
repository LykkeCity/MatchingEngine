package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.PriorityBlockingQueue

@Component
class MarketOrderValidatorImpl
@Autowired constructor(private val limitOrderInputValidator: LimitOrderInputValidator,
                       private val assetsPairsHolder: AssetsPairsHolder,
                       private val assetsHolder: AssetsHolder,
                       private val applicationSettingsCache: ApplicationSettingsCache) : MarketOrderValidator {

    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderValidatorImpl::class.java.name)
    }

    override fun performValidation(order: MarketOrder, orderBook: PriorityBlockingQueue<LimitOrder>,
                                   feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?) {
        isAssetKnown(order)
        isAssetEnabled(order)
        isVolumeValid(order)
        validateValue(order)
        isFeeValid(feeInstruction, feeInstructions, order)
        isOrderBookValid(order, orderBook)
        isVolumeAccuracyValid(order)
        isPriceAccuracyValid(order)
    }

    private fun isOrderBookValid(order: MarketOrder, orderBook: PriorityBlockingQueue<LimitOrder>) {
        if (orderBook.size == 0) {
            LOGGER.info("No liquidity, no orders in order book, for $order")
            throw OrderValidationException(OrderStatus.NoLiquidity)
        }
    }

    private fun isFeeValid(feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?, order: MarketOrder) {

        if (!checkFee(feeInstruction, feeInstructions)) {
            LOGGER.error("Invalid fee (order id: ${order.id}, order externalId: ${order.externalId})")
            throw OrderValidationException(OrderStatus.InvalidFee)
        }
    }

    private fun isVolumeValid(order: MarketOrder) {
        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, order.volume)) {
            val message = "volume can not be equal to zero"
            LOGGER.info(message)
            throw OrderValidationException(OrderStatus.InvalidVolume, message)
        }

        if (!OrderValidationUtils.checkMinVolume(order, assetsPairsHolder.getAssetPair(order.assetPairId))) {
            LOGGER.info("Too small volume for $order")
            throw OrderValidationException(OrderStatus.TooSmallVolume)
        }

        val assetPair = getAssetPair(order)
        if (order.isStraight() && assetPair.maxVolume != null && order.getAbsVolume() > assetPair.maxVolume) {
            LOGGER.info("Too large volume for $order")
            throw OrderValidationException(OrderStatus.InvalidVolume)
        }
    }

    private fun validateValue(order: MarketOrder) {
        val assetPair = getAssetPair(order)

        if (!order.isStraight() && assetPair.maxValue != null && order.getAbsVolume() > assetPair.maxValue) {
            LOGGER.info("Too large value for $order")
            throw OrderValidationException(OrderStatus.InvalidValue)
        }
    }

    private fun isAssetEnabled(order: MarketOrder) {
        val assetPair = getAssetPair(order)
        if (applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId)
                || applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
            LOGGER.info("Disabled asset $order")
            throw OrderValidationException(OrderStatus.DisabledAsset)
        }
    }

    private fun isAssetKnown(order: MarketOrder) {
        try {
            getAssetPair(order)
        } catch (e: Exception) {
            LOGGER.warn("Exception fetching asset", e)
            LOGGER.info("Unknown asset: ${order.assetPairId}")
            throw OrderValidationException(OrderStatus.UnknownAsset, order.assetPairId)
        }
    }

    private fun isVolumeAccuracyValid(order: MarketOrder) {
        val baseAssetVolumeAccuracy = assetsHolder.getAsset(getBaseAsset(order)).accuracy
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetVolumeAccuracy)

        if (!volumeAccuracyValid) {
            LOGGER.info("Volume accuracy invalid form base assetId: $baseAssetVolumeAccuracy, volume: ${order.volume}")
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy)
        }
    }

    private fun isPriceAccuracyValid(order: MarketOrder) {
        val price = order.price ?: return

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(price, getAssetPair(order).accuracy)

        if (!priceAccuracyValid) {
            LOGGER.info("Invalid order accuracy, ${order.assetPairId}, price: ${order.price}")
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy)
        }
    }

    private fun getBaseAsset(order: MarketOrder): String {
        val assetPair = getAssetPair(order)
        return if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId
    }

    private fun getAssetPair(order: MarketOrder) = assetsPairsHolder.getAssetPair(order.assetPairId)
}