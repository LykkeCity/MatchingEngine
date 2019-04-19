package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.services.events.NewAssetPairsEvent
import com.lykke.matching.engine.services.events.RemovedAssetPairsEvent
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.MockitoJUnitRunner
import org.springframework.context.ApplicationEventPublisher
import org.mockito.ArgumentCaptor
import kotlin.test.assertNull


@RunWith(MockitoJUnitRunner::class)
class AssetPairsCacheTest {

    @Mock
    private lateinit var dictionariesDatabaseAccessor: DictionariesDatabaseAccessor

    @Mock
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @Test
    fun testInitialLoading() {
        //given
        val btcUsdAssetPair = AssetPair("BTCUSD", "BTC", "USD", 5)
        val eurUsdAssetPair = AssetPair("EURUSD", "EUR", "USD", 2)

        Mockito.`when`(dictionariesDatabaseAccessor.loadAssetPairs())
                .thenReturn(hashMapOf("BTCUSD" to btcUsdAssetPair,
                        "EURUSD" to eurUsdAssetPair))

        //when
        val assetPairsCache = AssetPairsCache(dictionariesDatabaseAccessor, applicationEventPublisher)

        //then
        assertEquals(btcUsdAssetPair, assetPairsCache.getAssetPair("BTCUSD"))
        assertEquals(eurUsdAssetPair, assetPairsCache.getAssetPair("EURUSD"))
    }

    @Test
    fun testNewAssetAdded() {
        //given
        val btcUsdAssetPair = AssetPair("BTCUSD", "BTC", "USD", 5)
        val eurUsdAssetPair = AssetPair("EURUSD", "EUR", "USD", 2)

        Mockito.`when`(dictionariesDatabaseAccessor.loadAssetPairs())
                .thenReturn(hashMapOf("BTCUSD" to btcUsdAssetPair))

        //when
        val assetPairsCache = AssetPairsCache(dictionariesDatabaseAccessor, applicationEventPublisher, 50)
        assertEquals(btcUsdAssetPair, assetPairsCache.getAssetPair("BTCUSD"))

        Mockito.`when`(dictionariesDatabaseAccessor.loadAssetPairs())
                .thenReturn(hashMapOf("BTCUSD" to btcUsdAssetPair,
                        "EURUSD" to eurUsdAssetPair))
        Thread.sleep(100)


        //then
        assertEquals(eurUsdAssetPair, assetPairsCache.getAssetPair("EURUSD"))
        assertEquals(btcUsdAssetPair, assetPairsCache.getAssetPair("BTCUSD"))
        val eventCaptor = ArgumentCaptor.forClass(NewAssetPairsEvent::class.java)
        Mockito.verify(applicationEventPublisher).publishEvent(eventCaptor.capture())
        assertEquals("EURUSD", eventCaptor.value.assetPairs.single().assetPairId)
    }

    @Test
    fun testAssetRemoved() {
        //given
        val btcUsdAssetPair = AssetPair("BTCUSD", "BTC", "USD", 5)
        val eurUsdAssetPair = AssetPair("EURUSD", "EUR", "USD", 2)

        Mockito.`when`(dictionariesDatabaseAccessor.loadAssetPairs())
                .thenReturn(hashMapOf("BTCUSD" to btcUsdAssetPair,
                        "EURUSD" to eurUsdAssetPair))

        //when
        val assetPairsCache = AssetPairsCache(dictionariesDatabaseAccessor, applicationEventPublisher, 50)
        assertEquals(btcUsdAssetPair, assetPairsCache.getAssetPair("BTCUSD"))
        assertEquals(eurUsdAssetPair, assetPairsCache.getAssetPair("EURUSD"))

        Mockito.`when`(dictionariesDatabaseAccessor.loadAssetPairs())
                .thenReturn(hashMapOf("BTCUSD" to btcUsdAssetPair))
        Thread.sleep(100)


        //then
        assertEquals(btcUsdAssetPair, assetPairsCache.getAssetPair("BTCUSD"))
        assertNull(assetPairsCache.getAssetPair("EURUSD"))
        val eventCaptor = ArgumentCaptor.forClass(RemovedAssetPairsEvent::class.java)
        Mockito.verify(applicationEventPublisher).publishEvent(eventCaptor.capture())
        assertEquals("EURUSD", eventCaptor.value.assetPairs.single().assetPairId)
    }
}