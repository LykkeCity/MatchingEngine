package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import java.math.BigDecimal
import kotlin.math.sign

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

        fun isMidPriceValid(midPrice: BigDecimal?, lowerAcceptableMidPrice: BigDecimal?, upperAcceptableMidPrice: BigDecimal?): Boolean {
            if (lowerAcceptableMidPrice != null && lowerAcceptableMidPrice != BigDecimal.ZERO && upperAcceptableMidPrice != null
                    && upperAcceptableMidPrice != BigDecimal.ZERO && lowerAcceptableMidPrice.compareTo(upperAcceptableMidPrice).sign != -1) {
                throw IllegalArgumentException("Invalid data supplied for mid price validation, mid price $midPrice," +
                        " lower bound: $lowerAcceptableMidPrice, upper bound: $upperAcceptableMidPrice")
            }

            if (midPrice == null || midPrice == BigDecimal.ZERO ||
                    lowerAcceptableMidPrice == null  || lowerAcceptableMidPrice == BigDecimal.ZERO ||
                    upperAcceptableMidPrice == null  || upperAcceptableMidPrice == BigDecimal.ZERO ||
                    midPrice in lowerAcceptableMidPrice..upperAcceptableMidPrice) {
                return true
            }

            return false
        }
    }
}