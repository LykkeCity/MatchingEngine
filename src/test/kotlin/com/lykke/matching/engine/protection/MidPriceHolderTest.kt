package com.lykke.matching.engine.protection

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.monitoring.OrderBookMidPriceChecker
import org.apache.log4j.Logger
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

    @Autowired
    private lateinit var executionContextFactory: ExecutionContextFactory

    @Test
    fun initialLoadingTest() {
        //given
        val midPrices = getRandomMidPrices(100, "EURUSD")
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun noReferenceMidPriceIfDataIsNotYetCollected() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPriceHolder = MidPriceHolder(40, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        midPriceHolder.addMidPrice(assetPair, BigDecimal.valueOf(10), getExecutionContext(Date()))
        midPriceHolder.addMidPrice(assetPair, BigDecimal.valueOf(8), getExecutionContext(Date()))

        //then
        assertEquals(BigDecimal.ZERO, midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
        Thread.sleep(50)
        assertEquals(BigDecimal.valueOf(9), midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
    }

    @Test
    fun noReferenceMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //then
        assertEquals(BigDecimal.ZERO, midPriceHolder.getReferenceMidPrice(assetsPairsHolder.getAssetPair("EURUSD"), getExecutionContext(Date())))
    }

    @Test
    fun addFirstMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        val midPrice = BigDecimal("1.11")
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        midPriceHolder.addMidPrice(assetPair, midPrice, getExecutionContext(Date()))
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))

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
        val date = Date()

        val midPrice = MidPrice("EURUSD", getRandomBigDecimal(), date.time)
        midPrices.add(midPrice)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        midPriceHolder.addMidPrice(assetPair, midPrice.midPrice, getExecutionContext(Date()))
        Thread.sleep(150)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun accumulationOfCalculationErrorTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 900).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice, getExecutionContext(Date()))
        }

        Thread.sleep(150)

        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
    }

    @Test
    fun fullRecalculationTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 1100).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice, getExecutionContext(Date()))
        }

        Thread.sleep(150)
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
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
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, getExecutionContext(Date())) }

        Thread.sleep(50)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))
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
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, getExecutionContext(Date())) }

        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))
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
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))
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
        val referenceMidPriceEURUSD = midPriceHolder.getReferenceMidPrice(assetPairEURUSD, getExecutionContext(Date()))
        val referenceMidPriceEURCHF = midPriceHolder.getReferenceMidPrice(assetPairEURCHF, getExecutionContext(Date()))

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
        val clearedReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))
        midPriceHolder.addMidPrice(assetPair, BigDecimal.TEN, getExecutionContext(Date()))
        Thread.sleep(150)

        val newReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()))

        //then
        assertEquals(BigDecimal.ZERO, clearedReferenceMidPrice)
        assertEquals(BigDecimal.TEN, newReferenceMidPrice)
    }

    @Test
    fun prevRefMidPriceUsedIfNoNewMidPriceAvailable() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)

        //when
        Thread.sleep(100)


        //then
        val expectedReferencePrice = getExpectedReferencePrice(midPrices, assetPair.accuracy)
        assertEquals(expectedReferencePrice, midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
        assertEquals(expectedReferencePrice, midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date())))
    }

    @Test
    fun getRefMidPriceWithNotSavedCurrentMidPricesExistTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPrices = ArrayList(getRandomMidPrices(1, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        Thread.sleep(100)

        //when
        val savedMidPrices = getRandomBigDecimalList(3)
        val notSavedMidPrices = getRandomBigDecimalList(3)

        val sum = notSavedMidPrices.reduceRight { num, acc -> num.add(acc) }
        midPriceHolder.addMidPrices(assetPair, savedMidPrices, getExecutionContext(Date()))


        val refMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()), sum, BigDecimal.valueOf(notSavedMidPrices.size.toLong()))
        //then
        assertEquals(getExpectedRefPrice(savedMidPrices.plus(notSavedMidPrices), assetPair.accuracy), refMidPrice)
    }

    @Test
    fun getRefMidPriceWithNotSavedCurrentMidPricesDoesNotExistTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPrices = ArrayList(getRandomMidPrices(1, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, orderBookMidPriceChecker)
        Thread.sleep(100)

        //when
        val notSavedMidPrices = getRandomBigDecimalList(3)
        val sum = notSavedMidPrices.reduceRight { num, acc -> num.add(acc) }

        val refMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, getExecutionContext(Date()), sum, BigDecimal.valueOf(notSavedMidPrices.size.toLong()))
        //then
        assertEquals(getExpectedRefPrice(notSavedMidPrices, assetPair.accuracy), refMidPrice)
    }

    private fun getRandomBigDecimalList(size: Int): List<BigDecimal> {
        val result = ArrayList<BigDecimal>()
        IntRange(0, size - 1).forEach { result.add(getRandomBigDecimal()) }
        return result
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

    private fun getExpectedRefPrice(midPrices: List<BigDecimal>, accuracy: Int): BigDecimal {
        var sum = BigDecimal.ZERO
        midPrices.forEach { sum += it }

        return NumberUtils.setScaleRoundUp(NumberUtils.divideWithMaxScale(sum, BigDecimal.valueOf(midPrices.size.toLong())), accuracy)
    }

    private fun getExpectedReferencePrice(midPrices: List<MidPrice>, accuracy: Int): BigDecimal {
        var sum = BigDecimal.ZERO
        midPrices.forEach { sum += it.midPrice }

        return NumberUtils.setScaleRoundUp(NumberUtils.divideWithMaxScale(sum, BigDecimal.valueOf(midPrices.size.toLong())), accuracy)
    }

    private fun getExecutionContext(date: Date): ExecutionContext {
        return executionContextFactory.create("test", "test", MessageType.LIMIT_ORDER, null, emptyMap(), date, Logger.getLogger(""), Logger.getLogger(""))
    }
}