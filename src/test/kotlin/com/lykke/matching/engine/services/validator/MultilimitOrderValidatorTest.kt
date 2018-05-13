package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MultilimitOrderValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultilimitOrderValidatorTest {

    companion object {
        val CLIENT_NAME = "Client"
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
            return testBackOfficeDatabaseAccessor
        }

        @Bean
        open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair( ASSET_PAIR_ID, BASE_ASSET_ID, QUOTING_ASSET_ID, 2, 0.9))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var multiLimitOrderValidator: MultiLimitOrderValidator

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Test(expected = OrderValidationException::class)
    fun testInvalidPrice() {
        //given
        val order = getOrder()
        order.price = -1.0

        //when
        try {
            multiLimitOrderValidator.performValidation(order, assetsPairsHolder.getAssetPair(ASSET_PAIR_ID), getOrderBook())
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidPrice, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolume() {
        //given
        val order = getOrder(0.8)

        //when
        try {
            multiLimitOrderValidator.performValidation(order, assetsPairsHolder.getAssetPair(ASSET_PAIR_ID), getOrderBook())
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidSpread() {
        //given
        val order = getOrder(0.8)

        //when
        try {
            multiLimitOrderValidator.performValidation(order, assetsPairsHolder.getAssetPair(ASSET_PAIR_ID), getOrderBook())
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }



    private fun getOrder(volume: Double = 1.0): NewLimitOrder {
        return NewLimitOrder("test", "test", ASSET_PAIR_ID, CLIENT_NAME, volume, 1.0, "TEST",
                Date(), Date(), 0.1, null, null, null, null,
                null, null, null, null, null, null)
    }

    private fun getOrderBook(): AssetOrderBook {
             val assetOrderBook = AssetOrderBook(ASSET_PAIR_ID)
            assetOrderBook.addOrder(NewLimitOrder("test", "test",
                    ASSET_PAIR_ID, CLIENT_NAME, 1.0, 1.0,
                    OrderStatus.InOrderBook.name, Date(), Date(), 1.0, Date(), 1.0,
                    null, null, null, null, null, null, null, null))

            return assetOrderBook
    }
}