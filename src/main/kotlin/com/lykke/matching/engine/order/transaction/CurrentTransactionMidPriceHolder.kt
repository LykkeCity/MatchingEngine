package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import java.math.BigDecimal
import java.util.*

class CurrentTransactionMidPriceHolder(private val midPriceHolder: MidPriceHolder,
                                       private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) {
    private val midPriceByAssetPair = HashMap<String, MutableList<BigDecimal>>()
    private val midPricesSumByAssetPair = HashMap<String, BigDecimal>()
    private var removeAll = false

    fun addMidPrice(assetPairId: String, midPrice: BigDecimal, executionContext: ExecutionContext) {
        if (!isPersistOfMidPricesNeeded(assetPairId, executionContext)) {
            return
        }

        val midPrices = midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }
        midPrices.add(midPrice)
        val currentMidPriceSum = midPricesSumByAssetPair.getOrPut(assetPairId) { BigDecimal.ZERO }
        midPricesSumByAssetPair[assetPairId] = currentMidPriceSum + midPrice
    }

    fun addMidPrices(midPricesByAssetPairId: Map<String, BigDecimal>, executionContext: ExecutionContext) {
        midPricesByAssetPairId.forEach { assetPairId, midPrice ->
            addMidPrice(assetPairId, midPrice, executionContext)
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

    fun getRefMidPriceWithMidPricesFromCurrentTransaction(assetPair: AssetPair, executionContext: ExecutionContext): BigDecimal {
        return midPriceHolder.getReferenceMidPrice(assetPair,
                executionContext,
                midPricesSumByAssetPair[assetPair.assetPairId] ?: BigDecimal.ZERO,
                midPriceByAssetPair[assetPair.assetPairId]?.size ?: 0)
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
        midPriceByAssetPair.forEach { assetPairId, midPrices ->
            val assetPair = executionContext.assetPairsById[assetPairId]
            assetPair!!
            midPriceHolder.addMidPrice(assetPair, midPrices.last(), executionContext)
        }
    }

    private fun isPersistOfMidPricesNeeded(assetPairId: String, executionContext: ExecutionContext): Boolean {
        return priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPairId, executionContext) != null
    }
}