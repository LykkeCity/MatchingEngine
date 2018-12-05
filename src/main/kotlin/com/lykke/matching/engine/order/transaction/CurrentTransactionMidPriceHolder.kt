package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import java.math.BigDecimal
import java.util.*

class CurrentTransactionMidPriceHolder(private val midPriceHolder: MidPriceHolder,
                                       private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) {
    private val midPriceByAssetPair = HashMap<String, MutableList<BigDecimal>>()
    private var removeAll = false

    fun addMidPrice(assetPairId: String, midPrice: BigDecimal, executionContext: ExecutionContext) {
        if (!isPersistOfMidPricesNeeded(assetPairId, executionContext)) {
            return
        }

        val midPrices = midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }
        midPrices.add(midPrice)
    }

    fun addMidPrices(midPricesByAssetPairId: Map<String, List<BigDecimal>>, executionContext: ExecutionContext) {
        midPricesByAssetPairId.forEach { assetPairId, midPrices ->
            if (!isPersistOfMidPricesNeeded(assetPairId, executionContext)) {
                return@forEach
            }
            (midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }).addAll(midPrices)
        }
    }

    fun getPersistenceData(date: Date): MidPricePersistenceData {
        val midPricesList = ArrayList<MidPrice>()

        midPriceByAssetPair.forEach { assetPairId, midPrices ->
            midPrices.forEachIndexed { index, element ->
                midPricesList.add(MidPrice(assetPairId, element, date.time + index))
            }
        }

        return MidPricePersistenceData(midPricesList, removeAll)
    }

    fun getRefMidPrice(assetPairId: String, executionContext: ExecutionContext): BigDecimal {
        return midPriceHolder.getReferenceMidPrice(executionContext.assetPairsById[assetPairId]!!, executionContext)
    }

    fun setRemoveAllFlag() {
        midPriceByAssetPair.clear()
        removeAll = true
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

    private fun isPersistOfMidPricesNeeded(assetPairId: String, executionContext: ExecutionContext): Boolean {
        return priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPairId, executionContext) != null
    }
}