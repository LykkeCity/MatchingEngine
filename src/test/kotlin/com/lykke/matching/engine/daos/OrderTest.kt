package com.lykke.matching.engine.daos

import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.holders.AssetsPairsHolder
import org.junit.Before
import java.math.BigDecimal


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