package com.lykke.matching.engine.daos

import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OrderTest {

    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor))

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 0.1, 0.2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
    }

    @Test
    fun testCheckVolume() {
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 0.1).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 0.00000001).checkVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(volume = 1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(volume = 0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(volume = 0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(volume = -1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(volume = -0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(volume = -0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(price = 1.0, volume = 1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(price = 1.0, volume = 0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(price = 1.0, volume = 0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(price = 1.0, volume = -1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(price = 1.0, volume = -0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(price = 1.0, volume = -0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 0.1).checkVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 0.00000001).checkVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = 1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(volume = 0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = -1.0).checkVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(volume = -0.1).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.09).checkVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = 1.0, straight = false).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.1, straight = false).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.09, straight = false).checkVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = -1.0, straight = false).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.1, straight = false).checkVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.09, straight = false).checkVolume(assetsPairsHolder) }

    }
}