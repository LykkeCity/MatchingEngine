package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.Date
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

            if (midPrice == null || NumberUtils.equalsIgnoreScale(midPrice, BigDecimal.ZERO) ||
                    lowerAcceptableMidPrice == null || NumberUtils.equalsIgnoreScale(lowerAcceptableMidPrice, BigDecimal.ZERO) ||
                    upperAcceptableMidPrice == null || NumberUtils.equalsIgnoreScale(upperAcceptableMidPrice, BigDecimal.ZERO) ||
                    midPrice in lowerAcceptableMidPrice..upperAcceptableMidPrice) {
                return true
            }

            return false
        }

        fun checkExecutionPriceDeviation(isBuySide: Boolean,
                                         price: BigDecimal,
                                         expectedPrice: BigDecimal?,
                                         threshold: BigDecimal?): Boolean {
            if (threshold == null || expectedPrice == null) {
                return true
            }
            if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, expectedPrice)) {
                return false
            }
            return if (isBuySide) {
                NumberUtils.divideWithMaxScale(price - expectedPrice, expectedPrice) <= threshold
            } else {
                NumberUtils.divideWithMaxScale(expectedPrice - price, expectedPrice) <= threshold
            }
        }

        fun validateExpiration(order: LimitOrder, orderProcessingTime: Date) {
            if (order.isExpired(orderProcessingTime)) {
                throw OrderValidationException(OrderStatus.Cancelled, "expired")
            }
        }
    }
}