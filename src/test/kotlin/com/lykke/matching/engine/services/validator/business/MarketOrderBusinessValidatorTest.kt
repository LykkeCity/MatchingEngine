package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validator.input.MarketOrderInputValidatorTest
import com.lykke.matching.engine.services.validators.business.MarketOrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderBusinessValidatorTest {

    @Autowired
    private lateinit var marketOrderBusinessValidator: MarketOrderBusinessValidator

    @Test(expected = OrderValidationException::class)
    fun invalidOrderBook() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderBusinessValidator.performValidation(order)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.NoLiquidity, e.orderStatus)
            throw e
        }
    }

    private fun toMarketOrder(message: ProtocolMessages.MarketOrder): MarketOrder {
        val now = Date()
        return MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                OrderStatus.Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                NewFeeInstruction.create(message.fee), listOf(NewFeeInstruction.create(message.fee)))
    }

    private fun getOrderBook(isBuy: Boolean): PriorityBlockingQueue<LimitOrder> {
        val assetOrderBook = AssetOrderBook(MarketOrderInputValidatorTest.ASSET_PAIR_ID)
        val now = Date()
        assetOrderBook.addOrder(LimitOrder("test", "test",
                MarketOrderInputValidatorTest.ASSET_PAIR_ID, MarketOrderInputValidatorTest.CLIENT_NAME, BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.0),
                OrderStatus.InOrderBook.name, now, now, now, BigDecimal.valueOf(1.0), now, BigDecimal.valueOf(1.0),
                null, null, null, null, null, null, null, null,
                null, null, null, null))

        return assetOrderBook.getOrderBook(isBuy)
    }

    private fun getDefaultMarketOrderBuilder(): ProtocolMessages.MarketOrder.Builder {
        return ProtocolMessages.MarketOrder.newBuilder()
                .setUid(MarketOrderInputValidatorTest.OPERATION_ID)
                .setAssetPairId("EURUSD")
                .setTimestamp(System.currentTimeMillis())
                .setClientId(MarketOrderInputValidatorTest.CLIENT_NAME)
                .setVolume(1.0)
                .setStraight(true)
                .setFee(getFeeInstruction())
    }

    private fun getFeeInstruction(): ProtocolMessages.Fee {
        return ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.NO_FEE.externalId)
                .build()
    }
}