package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Test
import java.math.BigDecimal
import java.util.*

class LimitOrderBusinessValidatorTest {

    private companion object {
        private val ASSET_PAIR = AssetPair("AssetPair",
                "Asset1",
                "Asset2",
                2)
        private val MAX_VALUE_ASSET_PAIR = AssetPair("AssetPair",
                "Asset1",
                "Asset2",
                2,
                maxValue = BigDecimal.valueOf(5.0))
    }

    @Test(expected = OrderValidationException::class)
    fun testPreviousOrderNotFount() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(true,
                    getLimitOrder(status = OrderStatus.NotFoundPrevious.name, fee = getValidFee()),
                    BigDecimal.valueOf(12.0),
                    BigDecimal.valueOf(11.0),
                    ASSET_PAIR,
                    getValidOrderBook(),
                    Date())
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotFoundPrevious, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testNotEnoughFounds() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(true,
                    getLimitOrder(status = OrderStatus.NotEnoughFunds.name, fee = getValidFee()),
                    BigDecimal.valueOf(12.0),
                    BigDecimal.valueOf(11.0),
                    ASSET_PAIR,
                    getValidOrderBook(),
                    Date())
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotEnoughFunds, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidOrder() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        //when
        limitOrderBusinessValidatorImpl.performValidation(true,
                getLimitOrder(fee = getValidFee()),
                BigDecimal.valueOf(12.0),
                BigDecimal.valueOf(11.0),
                ASSET_PAIR,
                getValidOrderBook(),
                Date())
    }

    @Test(expected = OrderValidationException::class)
    fun testMaxVolume() {
        val validator = LimitOrderBusinessValidatorImpl()

        // orderBook with midPrice=2.0
        val orderBook = getValidOrderBook()
        orderBook.addOrder(buildLimitOrder(volume = -1.0, price = 3.0))
        orderBook.addOrder(buildLimitOrder(volume = 1.0, price = 1.0))

        try {
            validator.performValidation(false,
                    getLimitOrder(volume = BigDecimal.valueOf(-2.6), price = BigDecimal.valueOf(1.0)),
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    MAX_VALUE_ASSET_PAIR,
                    orderBook,
                    Date())
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidVolume, e.orderStatus)
            throw e
        }
    }

    private fun getLimitOrder(fee: LimitOrderFeeInstruction? = null,
                              fees: List<NewLimitOrderFeeInstruction>? = null,
                              assetPair: String = ASSET_PAIR.assetPairId,
                              price: BigDecimal = BigDecimal.valueOf(1.0),
                              volume: BigDecimal = BigDecimal.valueOf(1.0),
                              status: String = OrderStatus.InOrderBook.name): LimitOrder {
        return LimitOrder("test", "test", assetPair, "test", volume,
                price, status, Date(), Date(), Date(), BigDecimal.valueOf(1.0), null,
                expiryTime = null, timeInForce = null,
                type = LimitOrderType.LIMIT, fee = fee, fees = fees, lowerLimitPrice = null, lowerPrice = null, upperLimitPrice = null, upperPrice = null, previousExternalId = null,
                parentOrderExternalId = null,
                childOrderExternalId = null)
    }

    private fun getValidFee(): LimitOrderFeeInstruction {
        return LimitOrderFeeInstruction(FeeType.NO_FEE, null, null, null, null, null, null)
    }

    private fun getValidOrderBook(): AssetOrderBook {
        return AssetOrderBook(ASSET_PAIR.assetPairId)
    }
}