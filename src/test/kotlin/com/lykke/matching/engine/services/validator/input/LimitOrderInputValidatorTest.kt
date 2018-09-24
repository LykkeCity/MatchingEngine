package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.getSetting
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.lang.Exception
import java.math.BigDecimal
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderInputValidatorTest {
    companion object {
        val NON_EXISTENT_ASSET_PAIR = AssetPair("BTCOOO", "BTC", "OOO", 8)
        val DISABLED_ASSET_PAIR = AssetPair("JPYUSD", "JPY", "USD", 8)
        val MIN_VOLUME_ASSET_PAIR = AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2))
        val BTC_USD_ASSET_PAIR = AssetPair("BTCUSD", "BTC", "USD", 8, maxValue = BigDecimal.valueOf(10000.0))
    }

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    @Autowired
    private lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testConfigDatabaseAccessor(): TestSettingsDatabaseAccessor {
            val testConfigDatabaseAccessor = TestSettingsDatabaseAccessor()
            testConfigDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS, getSetting("JPY"))
            return testConfigDatabaseAccessor
        }
    }


    @Before
    fun init() {
        testDictionariesDatabaseAccessor.addAssetPair(MIN_VOLUME_ASSET_PAIR)
        testDictionariesDatabaseAccessor.addAssetPair(BTC_USD_ASSET_PAIR)
        testDictionariesDatabaseAccessor.addAssetPair(DISABLED_ASSET_PAIR)
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidFee() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee(), getNewLimitFee())))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidFee, e.orderStatus)
            throw e
        }
    }

    @Test(expected = Exception::class)
    @Ignore
    fun testAssetDoesNotExist() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), NON_EXISTENT_ASSET_PAIR.assetPairId))
        singleLimitContextBuilder.assetPair(null)
        singleLimitContextBuilder.baseAsset(null)
        singleLimitContextBuilder.quotingAsset(null)

        //when
        limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), NON_EXISTENT_ASSET_PAIR.assetPairId))
    }


    @Test(expected = OrderValidationException::class)
    @Ignore
    fun testInvalidAssets() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), "JPYUSD"))
        singleLimitContextBuilder.assetPair(DISABLED_ASSET_PAIR)
        singleLimitContextBuilder.baseAsset(Asset("JPY", 2))
        singleLimitContextBuilder.quotingAsset(Asset("USD", 2))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), DISABLED_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.DisabledAsset, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidPrice() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), price = BigDecimal.valueOf(-1.0)))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidPrice, e.orderStatus)
            throw e
        }
    }


    @Test(expected = OrderValidationException::class)
    fun testInvalidVolume() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), assetPair = "EURUSD", volume = BigDecimal.valueOf(0.001)))
        singleLimitContextBuilder.assetPair(MIN_VOLUME_ASSET_PAIR)

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), "EURUSD"))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testEmptyPrice() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getStopOrder(null, null, null, null))
        singleLimitContextBuilder.assetPair(MIN_VOLUME_ASSET_PAIR)

        //when
        try {
            limitOrderInputValidator.validateStopOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), MIN_VOLUME_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidPrice, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolumeEqualsZero() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), assetPair = "EURUSD", volume = BigDecimal.ZERO))
        singleLimitContextBuilder.assetPair(BTC_USD_ASSET_PAIR)

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), "EURUSD"))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidVolume, e.orderStatus)
            throw e
        }

    }


    @Test(expected = OrderValidationException::class)
    fun testInvalidPriceAccuracy() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), assetPair = "EURUSD", price = BigDecimal.valueOf(0.00000000001)))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), "EURUSD"))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidPriceAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidStopOrderPricesAccuracy() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), assetPair = "EURUSD",
                type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = BigDecimal.valueOf(0.000000001),
                lowerPrice = BigDecimal.valueOf(0.000000001)))

        //when
        try {
            limitOrderInputValidator.validateStopOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), "EURUSD"))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidPriceAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolumeAccuracy() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee()), volume = BigDecimal.valueOf(1.000000001)))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidVolumeAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidLimitPrice() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getStopOrder(lowerLimitPrice = BigDecimal.valueOf(9500.0), lowerPrice = BigDecimal.valueOf(9000.0),
                upperLimitPrice = BigDecimal.valueOf(9500.0), upperPrice = BigDecimal.valueOf(9100.0)))

        //when
        try {
            limitOrderInputValidator.validateStopOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidPrice, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidMaxValue() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(price = BigDecimal.valueOf(10000.0), volume = BigDecimal.valueOf(-1.1), fee = getFee()))

        //when
        try {
            limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.InvalidValue, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidLimitOrder() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getLimitOrder(fee = getFee()))

        //when
        limitOrderInputValidator.validateLimitOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
    }

    @Test
    fun testValidStopOrder() {
        //given
        val singleLimitContextBuilder = getSingleLimitContextBuilder()
        singleLimitContextBuilder.limitOrder(getStopOrder(BigDecimal.ONE, BigDecimal.ONE, null, null))

        //when
        limitOrderInputValidator.validateStopOrder(SingleLimitOrderParsedData(getMessageWrapper(singleLimitContextBuilder.build()), BTC_USD_ASSET_PAIR.assetPairId))
    }

    fun getMessageWrapper(singleLimitContext: SingleLimitOrderContext): MessageWrapper {
        return MessageWrapper("test", MessageType.LIMIT_ORDER.type, ByteArray(1), null, context = singleLimitContext)
    }

    fun getSingleLimitContextBuilder(): SingleLimitOrderContext.Builder {
        val builder = SingleLimitOrderContext.Builder()

        builder.messageId("test")
                .limitOrder(getLimitOrder(getFee(), listOf(getNewLimitFee())))
                .assetPair(BTC_USD_ASSET_PAIR)
                .baseAsset(Asset("BTC", 5))
                .quotingAsset(Asset("USD", 2))
                .trustedClient(false)
                .limitAsset(Asset("BTC", 5))
                .cancelOrders(false)
                .processedMessage(ProcessedMessage(MessageType.LIMIT_ORDER.type, 1, "String"))
        return builder
    }

    fun getStopOrder(lowerLimitPrice: BigDecimal?, lowerPrice: BigDecimal?, upperLimitPrice: BigDecimal?, upperPrice: BigDecimal?): LimitOrder {
        return LimitOrder("test", "test", "BTCUSD", "test", BigDecimal.ONE,
                BigDecimal.ONE, OrderStatus.InOrderBook.name, Date(), Date(), Date(), BigDecimal.ONE, null,
                fee = getFee(), type = LimitOrderType.STOP_LIMIT, fees = null,
                lowerLimitPrice = lowerLimitPrice, lowerPrice = lowerPrice,
                upperLimitPrice = upperLimitPrice, upperPrice = upperPrice, previousExternalId = null,
                timeInForce = null,
                expiryTime = null)
    }

    fun getLimitOrder(fee: LimitOrderFeeInstruction?,
                      fees: List<NewLimitOrderFeeInstruction>? = null,
                      assetPair: String = "BTCUSD",
                      price: BigDecimal = BigDecimal.valueOf(1.0),
                      volume: BigDecimal = BigDecimal.valueOf(1.0),
                      type: LimitOrderType = LimitOrderType.LIMIT,
                      lowerLimitPrice: BigDecimal? = null,
                      lowerPrice: BigDecimal? = null,
                      upperLimitPrice: BigDecimal? = null,
                      upperPrice: BigDecimal? = null): LimitOrder {
        return LimitOrder("test", "test", assetPair, "test", volume,
                price, OrderStatus.InOrderBook.name, Date(), Date(), Date(), BigDecimal.valueOf(1.0), null,
                type = type, fee = fee, fees = fees, lowerLimitPrice = lowerLimitPrice, lowerPrice = lowerPrice, upperLimitPrice = upperLimitPrice, upperPrice = upperPrice, previousExternalId = null,
                timeInForce = null,
                expiryTime = null)
    }

    fun getNewLimitFee(): NewLimitOrderFeeInstruction {
        return NewLimitOrderFeeInstruction(FeeType.NO_FEE, null, null, null, null, null, null, listOf(), null)
    }

    fun getFee(): LimitOrderFeeInstruction {
        return LimitOrderFeeInstruction(FeeType.NO_FEE, null, null, null, null, null, null)
    }
}