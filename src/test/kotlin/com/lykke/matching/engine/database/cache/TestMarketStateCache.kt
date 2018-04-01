package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.runners.MockitoJUnitRunner
import java.util.*

@RunWith(MockitoJUnitRunner::class)
class TestMarketStateCache {

    @Mock
    private lateinit var historyDatabaseAccessor: HistoryTicksDatabaseAccessor

    private lateinit var marketStateCache: MarketStateCache

//    private fun <T> any(): T {
//        Mockito.any<T>()
//        return uninitialized()
//    }
//    private fun <T> uninitialized(): T = null as T

    @Before
    fun init() {
        marketStateCache = MarketStateCache(historyDatabaseAccessor, 4000L)
    }

    @Test
    fun testAddDataToExistingTick() {
        val chfUsdTick = getTick("CHFUSD", LinkedList(Arrays.asList(0.3)), LinkedList(Arrays.asList(0.3)))
        val usdBtcTick = getTick("USDBTC", LinkedList(Arrays.asList(0.9)), LinkedList(Arrays.asList(0.8)))
        Mockito.`when`(historyDatabaseAccessor.loadHistoryTicks())
                .thenReturn(Arrays.asList(chfUsdTick,
                        usdBtcTick))

        marketStateCache.refresh()

        Thread.sleep(1000)

        val currentUpdateTime = System.currentTimeMillis()
        marketStateCache.addTick("CHFUSD", 0.5, 0.7, currentUpdateTime)
        marketStateCache.flush()

        val expectedTick = TickBlobHolder(chfUsdTick)
        expectedTick.addPrice(0.5, 0.7)

        verify(historyDatabaseAccessor).saveHistoryTick(expectedTick)
        verify(historyDatabaseAccessor, never()).saveHistoryTick(usdBtcTick)
    }

    @Test
    fun testAddDataToNonExistingTick() {

    }

    @Test
    fun testFlushChanges() {

    }

    @Test
    fun tickAddedOnTime() {

    }

    private fun getTick(assetPair: String, ask: LinkedList<Double>, bid: LinkedList<Double>): TickBlobHolder {
        return TickBlobHolder(assetPair = assetPair,
                tickUpdateInterval = TickUpdateInterval.ONE_HOUR,
                askTicks = ask,
                bidTicks = bid,
                lastUpdate = System.currentTimeMillis(),
                frequency =  4000L)
    }
}