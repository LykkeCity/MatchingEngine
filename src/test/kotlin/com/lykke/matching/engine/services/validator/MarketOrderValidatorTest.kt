package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderValidatorTest {

    companion object {
        val CLIENT_NAME = "Client"
        val OPERATION_ID = "test"
        val ASSET_PAIR_ID = "EURUSD"
        val BASE_ASSET_ID = "EUR"
        val QUOTING_ASSET_ID = "USD"
    }

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset(BASE_ASSET_ID, 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset(QUOTING_ASSET_ID, 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 2))
            return testBackOfficeDatabaseAccessor
        }

        @Bean
        open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair( ASSET_PAIR_ID, BASE_ASSET_ID, QUOTING_ASSET_ID, 2, BigDecimal.valueOf(0.9)))
            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        open fun test(): TestConfigDatabaseAccessor {
            val testConfigDatabaseAccessor = TestConfigDatabaseAccessor()
            testConfigDatabaseAccessor.addDisabledAsset("BTC")
            return testConfigDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var marketOrderValidator: MarketOrderValidator

    @Autowired
    private lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Test(expected = OrderValidationException::class)
    fun testUnknownAsset() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.assetPairId = "BTCUSD"
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.UnknownAsset, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testAssetDisabled() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.assetPairId = "BTCUSD"
        val order = toMarketOrder(marketOrderBuilder.build())
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair( "BTCUSD", "BTC", "USD", 2))

        //when
        try {
            marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.DisabledAsset, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolume() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.volume = 0.1
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(order,  getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidFee() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()),
                    NewFeeInstruction.create(getInvalidFee()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidFee, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun invalidOrderBook() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(order,  AssetOrderBook(ASSET_PAIR_ID).getOrderBook(order.isBuySide()),
                    NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.NoLiquidity, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolumeAccuracy() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.volume = 1.1111
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidVolumeAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidPriceAccuracy() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())
        order.price = BigDecimal.valueOf(1.1111)

        //when
        try {
            marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidPriceAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        marketOrderValidator.performValidation(order, getOrderBook(order.isBuySide()), NewFeeInstruction.create(getFeeInstruction()), null)
    }

    private fun toMarketOrder(message: ProtocolMessages.MarketOrder): MarketOrder {
        val now = Date()
        return MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                OrderStatus.Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                NewFeeInstruction.create(message.fee), listOf(NewFeeInstruction.create(message.fee)))
    }

    private fun getOrderBook(isBuy: Boolean): PriorityBlockingQueue<LimitOrder> {
        val assetOrderBook = AssetOrderBook(ASSET_PAIR_ID)
        val now = Date()
        assetOrderBook.addOrder(LimitOrder("test", "test",
                ASSET_PAIR_ID, CLIENT_NAME, BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.0),
                OrderStatus.InOrderBook.name, now, now, now, BigDecimal.valueOf(1.0), now, BigDecimal.valueOf(1.0),
                null, null, null, null, null, null, null, null))

        return assetOrderBook.getOrderBook(isBuy)
     }

    private fun getDefaultMarketOrderBuilder(): ProtocolMessages.MarketOrder.Builder {
        return ProtocolMessages.MarketOrder.newBuilder()
                .setUid(OPERATION_ID)
                .setAssetPairId("EURUSD")
                .setTimestamp(System.currentTimeMillis())
                .setClientId(CLIENT_NAME)
                .setVolume(1.0)
                .setStraight(true)
                .setFee(getFeeInstruction())
    }

    private fun getFeeInstruction(): ProtocolMessages.Fee {
        return  ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.NO_FEE.externalId)
                .build()
    }

    private fun getInvalidFee(): ProtocolMessages.Fee {
        return  ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.EXTERNAL_FEE.externalId)
                .build()
    }
}