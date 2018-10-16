package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class MultiLimitOrderValidatorImpl @Autowired constructor(private val assetsHolder: AssetsHolder,
                                                          private val assetsPairsHolder: AssetsPairsHolder,
                                                          private val limitOrderInputValidator: LimitOrderInputValidator): MultiLimitOrderValidator {
    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderValidatorImpl::class.java.name)
    }

    override fun performValidation(order: LimitOrder, assetPair: AssetPair, orderBook: AssetOrderBook) {
        isPriceValid(order)
        isVolumeValid(order)
        isSpreadValid(orderBook, order)
        isVolumeAccuracyValid(order, assetPair)
        isPriceAccuracyValid(order, assetPair)
        if (order.status != OrderStatus.InOrderBook.name) {
            throw OrderValidationException(OrderStatus.InOrderBook)
        }
    }

    private fun isSpreadValid(orderBook: AssetOrderBook, order: LimitOrder) {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to negative spread")
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread)
        }
    }

    private fun isPriceAccuracyValid(order: LimitOrder, assetPair: AssetPair) {
        val priceAccuracy = assetPair.accuracy

        val priceAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.price, priceAccuracy)

        if (!priceAccuracyValid) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price accuracy")
            throw OrderValidationException(OrderStatus.InvalidPriceAccuracy)
        }
    }

    private fun isVolumeAccuracyValid(order: LimitOrder, assetPair: AssetPair) {
        val baseAssetId = assetPair.baseAssetId
        val volumeAccuracy = assetsHolder.getAsset(baseAssetId).accuracy

        val volumeAccuracyValid = NumberUtils.isScaleSmallerOrEqual(order.volume, volumeAccuracy)

        if (!volumeAccuracyValid) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid volume accuracy")
            throw OrderValidationException(OrderStatus.InvalidVolumeAccuracy)
        }
    }

    private fun isVolumeValid(order: LimitOrder) {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)

        if (!OrderValidationUtils.checkMinVolume(order, assetsPairsHolder.getAssetPair(order.assetPairId))) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due too small volume")
            throw OrderValidationException(OrderStatus.TooSmallVolume)
        }

        if (assetPair.maxVolume != null && order.getAbsVolume() > assetPair.maxVolume) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to too large volume")
            throw OrderValidationException(OrderStatus.InvalidVolume)
        }
        if (assetPair.maxValue != null && order.getAbsVolume() * order.price > assetPair.maxValue) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to too large value")
            throw OrderValidationException(OrderStatus.InvalidValue)
        }
    }

    private fun isPriceValid(order: LimitOrder) {
        if (order.price <= BigDecimal.ZERO) {
            LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price")
            throw OrderValidationException(OrderStatus.InvalidPrice)
        }
    }
}