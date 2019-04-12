package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.order.MaxOrderVolumeInfo
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import java.math.BigDecimal
import java.util.Date

class OrderValidationUtils {
    companion object {
        fun checkMinVolume(order: Order, assetPair: AssetPair): Boolean {
            val volume = order.getAbsVolume()
            val minVolume = if (order.isStraight()) assetPair.minVolume else assetPair.minInvertedVolume
            return minVolume == null || volume >= minVolume
        }

        fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
            if (availableBalance < limitVolume) {
                throw OrderValidationException(OrderStatus.NotEnoughFunds, "not enough funds to reserve")
            }
        }

        fun validateExpiration(order: LimitOrder, orderProcessingTime: Date) {
            if (order.isExpired(orderProcessingTime)) {
                throw OrderValidationException(OrderStatus.Cancelled, "expired")
            }
        }

        fun validateMaxVolume(order: Order,
                                      assetPair: AssetPair,
                                      orderBook: AssetOrderBook) {
            val maxVolumeInfo = calculateMaxVolume(assetPair, orderBook) ?: return
            if (order.getAbsVolume() > maxVolumeInfo.maxVolume) {
                throw OrderValidationException(OrderStatus.InvalidVolume, "volume is too large ($maxVolumeInfo)")
            }
        }

        fun calculateMaxVolume(assetPair: AssetPair, orderBook: AssetOrderBook): MaxOrderVolumeInfo? {
            val midPrice = orderBook.getMidPrice() ?: return null
            val maxValue = assetPair.maxValue ?: return null
            return MaxOrderVolumeInfo(maxValue, midPrice)
        }
    }
}