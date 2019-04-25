package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.MarketOrderInputValidator
import com.lykke.matching.engine.utils.getSetting
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
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderInputValidatorTest: AbstractTest() {
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
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair(ASSET_PAIR_ID, BASE_ASSET_ID, QUOTING_ASSET_ID, 2, BigDecimal.valueOf(0.9)))
            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        open fun test(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS, getSetting("BTC"))
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var marketOrderInputValidator: MarketOrderInputValidator

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Test(expected = OrderValidationException::class)
    fun testUnknownAsset() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.assetPairId = "BTCUSD"
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderInputValidator.performValidation(getOrderContext(order))
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
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 2))

        //when
        try {
            marketOrderInputValidator.performValidation(getOrderContext(order))
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
            marketOrderInputValidator.performValidation(getOrderContext(order))
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidFee() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build(), getInvalidFee())

        //when
        try {
            marketOrderInputValidator.performValidation(getOrderContext(order))
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidFee, e.orderStatus)
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
            marketOrderInputValidator.performValidation(getOrderContext(order))
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
            marketOrderInputValidator.performValidation(getOrderContext(order))
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
        marketOrderInputValidator.performValidation(getOrderContext(order))
    }

    private fun getOrderContext(order: MarketOrder): MarketOrderContext {
        return messageBuilder.buildMarketOrderWrapper(order).context as MarketOrderContext
    }

    private fun toMarketOrder(message: ProtocolMessages.MarketOrder, fee: ProtocolMessages.Fee? = null): MarketOrder {
        val now = Date()
        return MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                OrderStatus.Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                NewFeeInstruction.create(fee ?: message.fee), emptyList())
    }

    private fun getOrderBook(isBuy: Boolean): PriorityBlockingQueue<LimitOrder> {
        val assetOrderBook = AssetOrderBook(ASSET_PAIR_ID)
        val now = Date()
        assetOrderBook.addOrder(LimitOrder("test", "test",
                ASSET_PAIR_ID, CLIENT_NAME, BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.0),
                OrderStatus.InOrderBook.name, now, now, now, BigDecimal.valueOf(1.0), now, BigDecimal.valueOf(1.0),
                null, null, null, null, null, null, null, null,
                null, null, null, null))

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
        return ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.NO_FEE.externalId)
                .build()
    }

    private fun getInvalidFee(): ProtocolMessages.Fee {
        return ProtocolMessages.Fee.newBuilder()
                .setType(FeeType.EXTERNAL_FEE.externalId)
                .build()
    }
}