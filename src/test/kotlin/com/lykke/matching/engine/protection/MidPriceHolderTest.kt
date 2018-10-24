package com.lykke.matching.engine.protection

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.assertEquals
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
import kotlin.test.assertNull


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

    @Test
    fun initialLoadingTest() {
        //given
        val midPrices = getRandomMidPrices(100, "EURUSD")
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun noReferenceMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)


        //then
        assertNull(midPriceHolder.getReferenceMidPrice(assetsPairsHolder.getAssetPair("EURUSD"), Date()))
    }

    @Test
    fun addFirstMidPriceTest() {
        //given
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        val midPrice = BigDecimal("1.11")
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val operationTime = Date()
        midPriceHolder.addMidPrice(assetPair, midPrice, operationTime)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, operationTime)

        //then
        assertEquals(midPrice, referenceMidPrice)
    }

    @Test
    fun addNotFirstMidPriceTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)
        val date = Date()

        val midPrice = MidPrice("EURUSD", getRandomBigDecimal(), date.time)
        midPrices.add(midPrice)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        midPriceHolder.addMidPrice(assetPair, midPrice.midPrice, date)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, date)

        //then
        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), referenceMidPrice)
    }

    @Test
    fun accumulationOfCalculationErrorTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 900).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice, date)
        }

        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, Date()))
    }

    @Test
    fun fullRecalculationTest() {
        //given
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPrices)

        //when
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        IntRange(0, 1100).forEach {
            val date = Date()
            val midPrice = getRandomBigDecimal()
            midPrices.add(MidPrice("EURUSD", midPrice, date.time))
            midPriceHolder.addMidPrice(assetPair, midPrice, date)
        }

        assertEquals(getExpectedReferencePrice(midPrices, assetPair.accuracy), midPriceHolder.getReferenceMidPrice(assetPair, Date()))
    }

    @Test
    fun notAllMidPricesExpiredTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPricesToExpire = ArrayList(getRandomMidPrices(3, "EURUSD"))
        testReadOnlyMidPriceDatabaseAccessor.addAll("EURUSD", midPricesToExpire)
        val midPriceHolder = MidPriceHolder(100, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        Thread.sleep(70)
        val notExpiredMidPrices = ArrayList(getRandomMidPrices(4, "EURUSD"))
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, Date()) }

        Thread.sleep(50)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())
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
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        Thread.sleep(100)
        val notExpiredMidPrices = ArrayList(getRandomMidPrices(4, "EURUSD"))
        notExpiredMidPrices.forEach { midPriceHolder.addMidPrice(assetPair, it.midPrice, Date()) }

        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())
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
        val midPriceHolder = MidPriceHolder(50, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        Thread.sleep(100)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())
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
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        val assetPairEURUSD = assetsPairsHolder.getAssetPair("EURUSD")
        val assetPairEURCHF = assetsPairsHolder.getAssetPair("EURCHF")
        val referenceMidPriceEURUSD = midPriceHolder.getReferenceMidPrice(assetPairEURUSD, Date())
        val referenceMidPriceEURCHF = midPriceHolder.getReferenceMidPrice(assetPairEURCHF, Date())

        //then
        assertEquals(getExpectedReferencePrice(midPricesEURUSD, assetPairEURUSD.accuracy), referenceMidPriceEURUSD)
        assertEquals(getExpectedReferencePrice(midPricesEURCHF, assetPairEURUSD.accuracy), referenceMidPriceEURCHF)
    }

    @Test
    fun clearTest() {
        //given
        val assetPair = assetsPairsHolder.getAssetPair("EURUSD")
        val midPrices = ArrayList(getRandomMidPrices(3, "EURUSD"))
        val midPriceHolder = MidPriceHolder(1000, testReadOnlyMidPriceDatabaseAccessor, assetsPairsHolder)

        //when
        midPriceHolder.clear()
        val clearedReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())
        midPriceHolder.addMidPrice(assetPair, BigDecimal.TEN, Date())
        val newReferenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, Date())

        //then
        assertEquals(null, clearedReferenceMidPrice)
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