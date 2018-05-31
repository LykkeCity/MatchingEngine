package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.PriorityBlockingQueue

@Component
class MarketOrderValidatorImpl
@Autowired constructor(private val assetsPairsHolder: AssetsPairsHolder,
                       private val assetsHolder: AssetsHolder,
                       private val assetSettingsCache: ApplicationSettingsCache) : MarketOrderValidator {

    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderValidatorImpl::class.java.name)
    }

    override fun performValidation(order: MarketOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>,
                                   feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?) {
        isAssetKnown(order)
        isAssetEnabled(order)
        isVolumeValid(order)
        isFeeValid(feeInstruction, feeInstructions, order)
        isOrderBookValid(order, orderBook)
        isVolumeAccuracyValid(order)
        isPriceAccuracyValid(order)
    }

    private fun isOrderBookValid(order: MarketOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>) {
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
        val assetPair = getAssetPair(order)
        if (!order.checkVolume(assetPair)) {
            LOGGER.info("Too small volume for $order")
            throw OrderValidationException(OrderStatus.TooSmallVolume)
        }
    }

    private fun isAssetEnabled(order: MarketOrder) {
        val assetPair = getAssetPair(order)
        if (assetSettingsCache.isAssetDisabled(assetPair.baseAssetId)
                || assetSettingsCache.isAssetDisabled(assetPair.quotingAssetId)) {
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

    private fun isVolumeAccuracyValid(order: MarketOrder){
        val baseAssetVolumeAccuracy = assetsHolder.getAsset(getBaseAsset(order)).accuracy
        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, baseAssetVolumeAccuracy)

        if (!volumeAccuracyValid) {

            order.status = OrderStatus.InvalidVolumeAccuracy.name
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