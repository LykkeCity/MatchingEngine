package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolderImpl
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.junit.Assert.assertEquals
import org.junit.Test
import java.math.BigDecimal
import java.util.*

class LimitOrderBusinessValidatorTest {

    private companion object {
        private const val ASSET_PAIR_ID = "BTCUSD"
    }

    @Test(expected = OrderValidationException::class)
    fun testPreviousOrderNotFount() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl(OrderBookMaxTotalSizeHolderImpl(null))

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(true,
                    getLimitOrder(status = OrderStatus.NotFoundPrevious.name, fee = getValidFee()),
                    BigDecimal.valueOf(12.0),
                    BigDecimal.valueOf(11.0),
                    getValidOrderBook(),
                    Date(),
                    0)
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotFoundPrevious, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testNotEnoughFounds() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl(OrderBookMaxTotalSizeHolderImpl(null))

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(true,
                    getLimitOrder(status = OrderStatus.NotEnoughFunds.name, fee = getValidFee()),
                    BigDecimal.valueOf(12.0),
                    BigDecimal.valueOf(11.0),
                    getValidOrderBook(),
                    Date(),
                    0)
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotEnoughFunds, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testOrderBookMaxTotalSize() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl(OrderBookMaxTotalSizeHolderImpl(10))

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(true,
                    getLimitOrder(fee = getValidFee()),
                    BigDecimal.valueOf(12.0),
                    BigDecimal.valueOf(11.0),
                    getValidOrderBook(),
                    Date(),
                    10)
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.OrderBookMaxSizeReached, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidOrder() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl(OrderBookMaxTotalSizeHolderImpl(null))

        //when
        limitOrderBusinessValidatorImpl.performValidation(true,
                getLimitOrder(fee = getValidFee()),
                BigDecimal.valueOf(12.0),
                BigDecimal.valueOf(11.0),
                getValidOrderBook(),
                Date(),
                0)
    }

    private fun getLimitOrder(fee: LimitOrderFeeInstruction?,
                              fees: List<NewLimitOrderFeeInstruction>? = null,
                              assetPair: String = ASSET_PAIR_ID,
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

    fun getValidFee(): LimitOrderFeeInstruction {
        return LimitOrderFeeInstruction(FeeType.NO_FEE, null, null, null, null, null, null)
    }

    private fun getValidOrderBook(): AssetOrderBook {
        return AssetOrderBook(ASSET_PAIR_ID)
    }
}