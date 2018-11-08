package com.lykke.matching.engine.protection

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.monitoring.OrderBookMidPriceChecker
import com.nhaarman.mockito_kotlin.doAnswer
import com.nhaarman.mockito_kotlin.mock
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


@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), MidPriceHolderTest.Config::class])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MidPriceHolderTest {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
            testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var testReadOnlyMidPriceDatabaseAccessor: TestReadOnlyMidPriceDatabaseAccessor

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    private lateinit var orderBookMidPriceChecker: OrderBookMidPriceChecker

    private var executionContextMock = mock<ExecutionContext> {
        on {date} doAnswer { Date() }
    }

    @Test
    fun initialLoadingTest() {
        //given
        val midPrices = getRandomMidPrices(100, "EURUSD")
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun noReferenceMidPriceIfDataIsNotYetCollected() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPriceHolder = MidPriceHolder(40, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        midPriceHolder.addMidPrice(assetPair, BigDecimal.valueOf(10), executionContextMock)
        midPriceHolder.addMidPrice(assetPair, BigDecimal.valueOf(8), executionContextMock)

        //then
        assertEquals(BigDecimal.ZERO, midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock))
        Thread.sleep(50)
        assertEquals(BigDecimal.valueOf(9), midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock))
    }

    @Test
    fun noReferenceMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //then
        assertEquals(BigDecimal.ZERO, midPriceHolder.getReferenceMidPrice(assetsPairsHolder.getAssetPair("EURUSD"),  executionContextMock))
    }

    @Test
    fun addFirstMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        val midPrice = BigDecimal("1.11")
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        midPriceHolder.addMidPrice(assetPair, midPrice, executionContextMock)
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)

        //then
        assertEquals(midPrice, referenceMidPrice)
    }

    @Test
    fun addNotFirstMidPriceTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        val midPrice = MidPrice("EURUSD", getRandomBigDecimal(), Date().time)
        midPrices.add(midPrice)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        midPriceHolder.addMidPrice(assetPair, midPrice.midPrice, executionContextMock)
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun accumulationOfCalculationErrorTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 900).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice,  executionContextMock)
        }

        Thread.sleep(1500)

        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock))
    }

    @Test
    fun fullRecalculationTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 1100).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice, executionContextMock)
        }

        Thread.sleep(1500)
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock))
    }

    @Test
    fun notAllMidPricesExpiredTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPricesToExpire = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPricesToExpire)
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        Thread.sleep(70)
        val notExpiredMidPrices = ArrayList(getRandomMidPrices(4, "EURUSD"))
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, executionContextMock) }

        Thread.sleep(50)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)
        val expectedReferencePrice = getExpectedReferencePrice(notExpiredMidPrices, assetPair.accuracy)

        //then
        assertEquals(expectedReferencePrice, referenceMidPrice)
    }

    @Test
    fun allMidPricesExpiredAddNewMidPricesTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPricesToExpire = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPricesToExpire)
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        Thread.sleep(100)
        val notExpiredMidPrices = ArrayList(getRandomMidPrices(4, "EURUSD"))
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, executionContextMock) }

        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)
        val expectedReferencePrice = getExpectedReferencePrice(notExpiredMidPrices, assetPair.accuracy)

        //then
        assertEquals(expectedReferencePrice, referenceMidPrice)
    }

    @Test
    fun allMidPricesExpiredUsePreviousReferenceMidPriceTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPricesToExpire = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPricesToExpire)
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        Thread.sleep(100)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)
        val expectedReferencePrice = getExpectedReferencePrice(midPricesToExpire, assetPair.accuracy)

        //then
        assertEquals(expectedReferencePrice, referenceMidPrice)
    }

    @Test
    fun testMultipleAssetPairs() {
        //given
        val midPricesEURUSD = ArrayList(getRandomMidPrices(3, "EURUSD"))
        val midPricesEURCHF = ArrayList(getRandomMidPrices(3, "EURCHF"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPricesEURUSD)
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURCHF", midPricesEURCHF)
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        Thread.sleep(150)

        //when
        val assetPairEURUSD = assetsPairsHolder.getAssetPair("EURUSD")
        val assetPairEURCHF = assetsPairsHolder.getAssetPair("EURCHF")
        val referenceMidPriceEURUSD = midPriceHolder.getReferenceMidPrice(assetPairEURUSD, executionContextMock)
        val referenceMidPriceEURCHF = midPriceHolder.getReferenceMidPrice(assetPairEURCHF, executionContextMock)

        //then
        assertEquals(getExpectedReferencePrice(midPricesEURUSD, assetPairEURUSD.accuracy), referenceMidPriceEURUSD)
        assertEquals(getExpectedReferencePrice(midPricesEURCHF, assetPairEURUSD.accuracy), referenceMidPriceEURCHF)
    }

    @Test
    fun clearTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        midPriceHolder.clear()
        Thread.sleep(150)
        val clearedReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)
        midPriceHolder.addMidPrice(assetPair, BigDecimal.TEN, executionContextMock)
        Thread.sleep(150)

        val newReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContextMock)

        //then
        assertEquals(BigDecimal.ZERO, clearedReferenceMidPrice)
        assertEquals(BigDecimal.TEN, newReferenceMidPrice)
    }

    private fun getRandomMidPrices(size: Int, assetId: String): List<MidPrice> {
        val result = ArrayList<MidPrice>()
        val start = Date().time
        IntRange(0, size - 1).forEach { result.add(MidPrice(assetId, getRandomBigDecimal(), start - it * 10)) }
        return result
    }

    private fun getRandomBigDecimal(): BigDecimal {
        return NumberUtils.setScale(BigDecimal.valueOf(Math.random() * 1000), 0, false)
    }

    private fun getExpectedReferencePrice(midPrices: List<MidPrice>, accuracy: Int): BigDecimal {
        var sum = BigDecimal.ZERO
        midPrices.forEach { sum += it.midPrice }

        return NumberUtils.setScaleRoundUp(NumberUtils.divideWithMaxScale(sum, BigDecimal.valueOf(midPrices.size.toLong())), accuracy)
    }
}