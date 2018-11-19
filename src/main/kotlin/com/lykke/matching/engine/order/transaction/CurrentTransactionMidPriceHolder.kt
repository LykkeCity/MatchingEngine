package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.holders.MidPriceHolder
import java.math.BigDecimal
import java.util.*

class CurrentTransactionMidPriceHolder(private val midPriceHolder: MidPriceHolder) {
    private val midPriceByAssetPair = HashMap<String, MutableList<BigDecimal>>()
    private var removeAll = false

    fun addMidPrice(assetPairId: String, midPrice: BigDecimal) {
        val midPrices = midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }
        midPrices.add(midPrice)
    }

    fun addMidPrices(midPricesByAssetPairId: Map<String, List<BigDecimal>>) {
        midPricesByAssetPairId.forEach { assetPairId, midPrices -> (midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }).addAll(midPrices) }
    }

    fun getPersistenceData(date: Date): MidPricePersistenceData {
        val midPricesList = ArrayList<MidPrice>()

        midPriceByAssetPair.forEach{ assetPairId, midPrices ->
            midPrices.forEachIndexed { index, element ->
                midPricesList.add(MidPrice(assetPairId, element, date.time + index))
            }
        }

        return MidPricePersistenceData(midPricesList , removeAll)
    }

    fun getRefMidPrice(assetPairId: String, executionContext: ExecutionContext): BigDecimal {
        return executionContext.assetPairsById[assetPairId]?.let {
            midPriceHolder.getReferenceMidPrice(executionContext.assetPairsById[assetPairId]!!,
                    executionContext,
                    midPriceByAssetPair[assetPairId] ?: emptyList())
        } ?: BigDecimal.ZERO
    }

    fun setRemoveAllFlag() {
        midPriceByAssetPair.clear()
        removeAll = true
    }

    fun getRefMidPricePeriod(): Long {
        return midPriceHolder.refreshMidPricePeriod
    }

    fun apply(executionContext: ExecutionContext) {
        if (removeAll) {
            midPriceHolder.clear()
            return
        }
        midPriceByAssetPair.forEach { assetPairId, midPrice ->
            val assetPair = executionContext.assetPairsById[assetPairId]
            assetPair!!
            midPrice.forEach {
                midPriceHolder.addMidPrice(assetPair, it, executionContext)
            }
        }
    }
}