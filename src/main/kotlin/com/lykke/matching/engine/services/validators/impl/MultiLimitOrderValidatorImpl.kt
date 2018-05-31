package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class MultiLimitOrderValidatorImpl @Autowired constructor(private val assetsHolder: AssetsHolder): MultiLimitOrderValidator {
    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderValidatorImpl::class.java.name)
    }

    override fun performValidation(order: NewLimitOrder, assetPair: AssetPair, orderBook: AssetOrderBook) {
        isPriceValid(order)
        isVolumeValid(order, assetPair)
        isSpreadValid(orderBook, order)
        isVolumeAccuracyValid(order, assetPair)
        isPriceAccuracyValid(order, assetPair)
    }

    private fun isSpreadValid(orderBook: AssetOrderBook, order: NewLimitOrder) {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to negative spread")
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread)
        }
    }

    private fun isPriceAccuracyValid(order: NewLimitOrder, assetPair: AssetPair) {
        val priceAccuracy = assetPair.accuracy

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.price, priceAccuracy)

        if (!priceAccuracyValid) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price accuracy")
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy)
        }
    }

    private fun isVolumeAccuracyValid(order: NewLimitOrder, assetPair: AssetPair) {
        val baseAssetId = assetPair.baseAssetId
        val volumeAccuracy = assetsHolder.getAsset(baseAssetId).accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, volumeAccuracy)

        if (!volumeAccuracyValid) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid volume accuracy")
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy)
        }
    }

    private fun isVolumeValid(order: NewLimitOrder, assetPair: AssetPair) {
        if (!order.checkVolume(assetPair)) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due too small volume")
            throw OrderValidationException(OrderStatus.TooSmallVolume)
        }
    }

    private fun isPriceValid(order: NewLimitOrder) {
        if (order.price <= 0) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price")
            throw OrderValidationException(OrderStatus.InvalidPrice)
        }
    }
}