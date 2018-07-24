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
}