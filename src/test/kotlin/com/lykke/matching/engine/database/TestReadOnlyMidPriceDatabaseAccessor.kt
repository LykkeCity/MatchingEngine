package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MidPrice
import redis.clients.jedis.Transaction
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.util.*
import kotlin.math.sign

class TestReadOnlyMidPriceDatabaseAccessor : MidPriceDatabaseAccessor {
    private val assetPairIdToMidPrices = HashMap<String, LinkedList<MidPrice>>()

    override fun all(): Map<String, List<MidPrice>> {
        return assetPairIdToMidPrices
                .mapValues { entry -> entry.value.sortWith(Comparator { midPrice1, midPrice2 -> (midPrice1.timestamp - midPrice2.timestamp).sign })
                    entry.value}
    }

    override fun removeAll(transaction: Transaction) {
        throw NotImplementedException()
    }

    override fun save(transaction: Transaction, midPrice: List<MidPrice>) {
        throw NotImplementedException()
    }

    fun addMidPrice(assetPairId: String, midPrice: MidPrice) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPairId) {LinkedList()}
        midPrices.addAll(listOf(midPrice))
    }

    fun addAll(assetPairId: String, prices: List<MidPrice>) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPairId) {LinkedList()}
        midPrices.addAll(prices)
    }
}