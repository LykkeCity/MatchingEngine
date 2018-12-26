package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import java.math.BigDecimal
import java.util.*

class CurrentTransactionMidPriceHolder(private val midPriceHolder: MidPriceHolder,
                                       private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) : MidPriceHolder {
    private val midPriceByAssetPair = HashMap<String, MutableList<BigDecimal>>()
    private val midPricesSumByAssetPair = HashMap<String, BigDecimal>()
    private var removeAll = false

    override fun addMidPrice(assetPair: AssetPair, newMidPrice: BigDecimal, executionContext: ExecutionContext) {
        if (!isPersistOfMidPricesNeeded(assetPair.assetPairId, executionContext)) {
            return
        }

        val midPrices = midPriceByAssetPair.getOrPut(assetPair.assetPairId) { ArrayList() }
        midPrices.add(newMidPrice)
        val currentMidPriceSum = midPricesSumByAssetPair.getOrPut(assetPair.assetPairId) { BigDecimal.ZERO }
        midPricesSumByAssetPair[assetPair.assetPairId] = currentMidPriceSum + newMidPrice
    }

    fun addMidPrices(midPricesByAssetPairId: Map<AssetPair, BigDecimal>, executionContext: ExecutionContext) {
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

    override fun getReferenceMidPrice(assetPair: AssetPair, executionContext: ExecutionContext,
                                      notSavedMidPricesSum: BigDecimal?,
                                      notSavedMidPricesLength: Int?): BigDecimal {
        val currentTransactionMidPriceSum = midPricesSumByAssetPair[assetPair.assetPairId] ?: BigDecimal.ZERO
        val currentTransactionMidPriceLength = midPriceByAssetPair[assetPair.assetPairId]?.size ?: 0
        val midPricesSum = notSavedMidPricesSum?.let {
            it.plus(currentTransactionMidPriceSum)
        } ?: currentTransactionMidPriceSum

        val midPriceLength = notSavedMidPricesLength?.let {
            it.plus(currentTransactionMidPriceLength)
        } ?: currentTransactionMidPriceLength

        return midPriceHolder.getReferenceMidPrice(assetPair,
                executionContext,
                midPricesSum,
                midPriceLength)
    }

    override fun clear() {
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