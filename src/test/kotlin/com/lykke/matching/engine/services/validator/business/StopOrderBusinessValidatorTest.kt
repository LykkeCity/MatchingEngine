package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolderImpl
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.impl.StopOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Test
import java.math.BigDecimal
import java.util.Date
import kotlin.test.assertEquals

class StopOrderBusinessValidatorTest {

    companion object {
        private val MAX_VALUE_ASSET_PAIR = AssetPair("AssetPair",
                "Asset1",
                "Asset2",
                2,
                maxValue = BigDecimal.valueOf(5.0))
    }

    private val validator = StopOrderBusinessValidatorImpl(OrderBookMaxTotalSizeHolderImpl(null))

    @Test(expected = OrderValidationException::class)
    fun testMaxVolume() {
        val stopOrder = buildStopOrder(volume = -2.6, lowerLimitPrice = 1.0, lowerPrice = 1.0)

        // orderBook with midPrice=2.0
        val orderBook = AssetOrderBook(MAX_VALUE_ASSET_PAIR.assetPairId)
        orderBook.addOrder(buildLimitOrder(volume = -1.0, price = 3.0))
        orderBook.addOrder(buildLimitOrder(volume = 1.0, price = 1.0))

        try {
            validator.performValidation(BigDecimal.ONE,
                    BigDecimal.ONE,
                    stopOrder,
                    Date(),
                    MAX_VALUE_ASSET_PAIR,
                    orderBook,
                    0)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidVolume, e.orderStatus)
            throw e
        }
    }

    private fun buildStopOrder(volume: Double,
                               lowerLimitPrice: Double? = null,
                               lowerPrice: Double? = null,
                               upperLimitPrice: Double? = null,
                               upperPrice: Double? = null): LimitOrder {
        return buildLimitOrder(type = LimitOrderType.STOP_LIMIT,
                volume = volume,
                lowerLimitPrice = lowerLimitPrice,
                lowerPrice = lowerPrice,
                upperLimitPrice = upperLimitPrice,
                upperPrice = upperPrice)
    }

}