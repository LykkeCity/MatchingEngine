package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice
import java.util.*
import kotlin.math.sign

class TestReadOnlyMidPriceDatabaseAccessor : ReadOnlyMidPriceDatabaseAccessor {
    private val assetPairIdToMidPrices = HashMap<String, LinkedList<MidPrice>>()

    override fun all(): Map<String, List<MidPrice>> {
        return assetPairIdToMidPrices
                .mapValues { entry -> entry.value.sortWith(Comparator { midPrice1, midPrice2 -> (midPrice1.timestamp - midPrice2.timestamp).sign })
                    entry.value}
    }

    fun addMidPrice(assetPairId: String, midPrice: MidPrice) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPairId) {LinkedList()}
        midPrices.add(midPrice)
    }

    fun addAll(assetPairId: String, prices: List<MidPrice>) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPairId) {LinkedList()}
        midPrices.addAll(prices)
    }
}