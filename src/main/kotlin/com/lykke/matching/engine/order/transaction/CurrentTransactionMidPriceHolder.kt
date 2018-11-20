package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.*

class CurrentTransactionMidPriceHolder(private val midPriceHolder: MidPriceHolder) {
    private val midPriceByAssetPair = HashMap<String, MutableList<BigDecimal>>()
    private val currentTransactionAvgMidPriceByAssetPair = HashMap<String, BigDecimal>()

    private var removeAll = false

    fun addMidPrice(assetPairId: String, midPrice: BigDecimal) {
        val midPrices = midPriceByAssetPair.getOrPut(assetPairId) { ArrayList() }
        updateCurrentTransactionAvgMidPrice(assetPairId, midPrice)
        midPrices.add(midPrice)
    }

    fun addMidPrices(midPricesByAssetPairId: Map<String, List<BigDecimal>>) {
        midPricesByAssetPairId.forEach { assetPairId, midPrices -> midPrices.forEach { addMidPrice(assetPairId, it) } }
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
        return executionContext.assetPairsById[assetPairId]?.let {
            val persistedRefMidPriceToSize = midPriceHolder.getReferenceMidPriceMidPriceSize(executionContext.assetPairsById[assetPairId]!!,
                    executionContext)

            val midPriceSize = BigDecimal.valueOf(persistedRefMidPriceToSize.second.toLong())

            val curTransactionAvgMidPrice = currentTransactionAvgMidPriceByAssetPair.get(assetPairId)
            val curTransactionMidPriceSize = BigDecimal.valueOf(midPriceByAssetPair[assetPairId]!!.size.toLong())

            if (curTransactionAvgMidPrice == null || NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, curTransactionAvgMidPrice)) {
                return persistedRefMidPriceToSize.first
            }


            val coef1 =  NumberUtils.divideWithMaxScale(midPriceSize, curTransactionMidPriceSize)
            val withNewMidPrices  = persistedRefMidPriceToSize.first.add(NumberUtils.divideWithMaxScale(curTransactionAvgMidPrice, coef1))
            val coef2 = NumberUtils.divideWithMaxScale(curTransactionMidPriceSize + midPriceSize, midPriceSize)
            NumberUtils.divideWithMaxScale(withNewMidPrices, coef2)

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

    private fun updateCurrentTransactionAvgMidPrice(assetPairId: String, midPrice: BigDecimal) {
        val currentAvgMidPrice = currentTransactionAvgMidPriceByAssetPair.getOrPut(assetPairId) { BigDecimal.ZERO }

        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, currentAvgMidPrice)) {
            currentTransactionAvgMidPriceByAssetPair[assetPairId] = midPrice
            return
        }

        val curMidPriceSize = BigDecimal.valueOf(midPriceByAssetPair[assetPairId]?.size?.toLong() ?: 0L)
        val newMidPrice = currentAvgMidPrice.add(NumberUtils.divideWithMaxScale(midPrice, curMidPriceSize))

        val coef = NumberUtils.divideWithMaxScale(curMidPriceSize, curMidPriceSize.add(BigDecimal.ONE))
        currentTransactionAvgMidPriceByAssetPair[assetPairId] = newMidPrice * coef
    }
}