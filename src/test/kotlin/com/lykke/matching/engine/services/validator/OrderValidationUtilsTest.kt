package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Assert
import org.junit.Test
import java.math.BigDecimal
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OrderValidationUtilsTest {
    private companion object {
        val MIN_VOLUME_ASSET_PAIR = AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2))
        val BTC_USD_ASSET_PAIR = AssetPair("BTCUSD", "BTC", "USD", 8)
    }
    
    @Test
    fun testCheckVolume() {
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 1.0), BTC_USD_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.1), BTC_USD_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(assetId = "BTCUSD", volume = 0.00000001), BTC_USD_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildLimitOrder(price = 1.0, volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 1.0), BTC_USD_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.1), BTC_USD_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(assetId = "BTCUSD", volume = 0.00000001), BTC_USD_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -1.0), MIN_VOLUME_ASSET_PAIR) }
        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.1), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.09), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 1.0, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.1, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = 0.09, straight = false), MIN_VOLUME_ASSET_PAIR) }

        assertTrue { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -1.0, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.1, straight = false), MIN_VOLUME_ASSET_PAIR) }
        assertFalse { OrderValidationUtils.checkMinVolume(MessageBuilder.buildMarketOrder(volume = -0.09, straight = false), MIN_VOLUME_ASSET_PAIR) }
    }


    @Test(expected = OrderValidationException::class)
    fun testInvalidBalance() {
        try {
            //when
            OrderValidationUtils.validateBalance(BigDecimal.valueOf(10.0), BigDecimal.valueOf(11.0))
        } catch (e: OrderValidationException) {
            //then
            Assert.assertEquals(OrderStatus.NotEnoughFunds, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidBalance() {
        //when
        OrderValidationUtils.validateBalance(BigDecimal.valueOf(10.0), BigDecimal.valueOf(9.0))
    }

    @Test
    fun testMidPrice() {
        assertTrue(OrderValidationUtils.isMidPriceValid(null, BigDecimal.valueOf(1), BigDecimal.valueOf(2)))
        assertTrue(OrderValidationUtils.isMidPriceValid(null, null, BigDecimal.valueOf(2)))
        assertTrue(OrderValidationUtils.isMidPriceValid(null, null, null))

        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.ZERO, BigDecimal.valueOf(9), BigDecimal.valueOf(11)))
        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(10), BigDecimal.ZERO, BigDecimal.valueOf(11)))
        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(10), BigDecimal.valueOf(9), BigDecimal.ZERO))


        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(10), BigDecimal.valueOf(9), BigDecimal.valueOf(11)))
        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(10), BigDecimal.valueOf(10), BigDecimal.valueOf(11)))
        assertTrue(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(11), BigDecimal.valueOf(10), BigDecimal.valueOf(11)))

        assertFalse(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(11), BigDecimal.valueOf(12), BigDecimal.valueOf(15)))
        assertFalse(OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(16), BigDecimal.valueOf(12), BigDecimal.valueOf(15)))
    }

    @Test(expected = IllegalArgumentException::class)
    fun testMidPriceSuppliedInvalidBounds() {
       OrderValidationUtils.isMidPriceValid(BigDecimal.valueOf(11), BigDecimal.valueOf(15), BigDecimal.valueOf(10))
    }
}