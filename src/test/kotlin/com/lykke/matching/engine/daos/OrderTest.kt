package com.lykke.matching.engine.daos

import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import org.junit.Before
import org.junit.Test
import java.math.BigDecimal
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OrderTest {

    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor))

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
    }

    @Test
    fun testCheckVolume() {
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 0.1).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(assetId = "BTCUSD", volume = 0.00000001).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(volume = 1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(volume = 0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(volume = 0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(volume = -1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(volume = -0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(volume = -0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(price = 1.0, volume = 1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(price = 1.0, volume = 0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(price = 1.0, volume = 0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildLimitOrder(price = 1.0, volume = -1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildLimitOrder(price = 1.0, volume = -0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildLimitOrder(price = 1.0, volume = -0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 0.1).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(assetId = "BTCUSD", volume = 0.00000001).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = 1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(volume = 0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = -1.0).checkMinVolume(assetsPairsHolder) }
        assertTrue { buildMarketOrder(volume = -0.1).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.09).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = 1.0, straight = false).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.1, straight = false).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = 0.09, straight = false).checkMinVolume(assetsPairsHolder) }

        assertTrue { buildMarketOrder(volume = -1.0, straight = false).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.1, straight = false).checkMinVolume(assetsPairsHolder) }
        assertFalse { buildMarketOrder(volume = -0.09, straight = false).checkMinVolume(assetsPairsHolder) }

    }
}