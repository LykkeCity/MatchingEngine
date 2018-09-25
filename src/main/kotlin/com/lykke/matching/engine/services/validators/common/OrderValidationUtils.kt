package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import java.math.BigDecimal
import java.util.*

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
    }
}