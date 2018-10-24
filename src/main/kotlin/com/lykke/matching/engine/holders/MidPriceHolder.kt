package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.MidPriceDatabaseAccessor
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class MidPriceHolder(@Value("#{Config.me.referenceMidPricePeriod}") private val refreshMidPricePeriod: Long,
                     midPriceDatabaseAccessor: MidPriceDatabaseAccessor,
                     assetsPairsHolder: AssetsPairsHolder) {

    private val MAX_MID_PRICE_RECALCULATION_COUNT = 1000

    private var assetPairIdToMidPrices = HashMap<String, LinkedList<MidPrice>>()
    private var assetPairIdToReferencePrice = HashMap<String, BigDecimal>()
    private var midPriceRecalculationCount = 0

    init {
        midPriceDatabaseAccessor
                .all()
                .forEach { assetPairId, midPrices -> assetPairIdToMidPrices[assetPairId] = LinkedList(midPrices) }

        assetPairIdToMidPrices.keys.forEach { key ->
            val assetPair = assetsPairsHolder.getAssetPairAllowNulls(key)
            if (assetPair != null) performFullRecalculationOfReferenceMidPrice(assetPair)
        }
    }

    fun getReferenceMidPrice(assetPair: AssetPair, operationTime: Date): BigDecimal? {
        removeObsoleteMidPrices(assetPair, getLowerTimeBound(operationTime.time))

        val result = assetPairIdToReferencePrice[assetPair.assetPairId]
        return if (result != null) {
            NumberUtils.setScaleRoundUp(result, assetPair.accuracy)
        } else {
            null
        }
    }

    fun addMidPrice(assetPair: AssetPair, midPrice: BigDecimal, operationTime: Date) {
        removeObsoleteMidPrices(assetPair, getLowerTimeBound(operationTime.time))
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair.assetPairId) { LinkedList() }

        recalculateReferenceMidPriceForNewAddedMidPrice(assetPair, midPrices, midPrice)
        midPrices.add(MidPrice(assetPair.assetPairId, midPrice, operationTime.time))
    }

    fun clear() {
        assetPairIdToMidPrices.clear()
        assetPairIdToReferencePrice.clear()
        midPriceRecalculationCount = 0
    }

    private fun removeObsoleteMidPrices(assetPair: AssetPair, lowerBoundTimeMillisBound: Long) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair.assetPairId) { LinkedList() }

        val initialSize = midPrices.size
        var removedMidPricesSum = BigDecimal.ZERO

        while (midPrices.size > 0 && midPrices.first().timestamp < lowerBoundTimeMillisBound) {
            removedMidPricesSum += midPrices.removeFirst().midPrice
        }

        recalculateReferenceMidPriceAfterRemoval(removedMidPricesSum, initialSize, midPrices.size, assetPair)
    }

    private fun recalculateReferenceMidPriceForNewAddedMidPrice(assetPair: AssetPair,
                                                                midPrices: MutableList<MidPrice>,
                                                                midPrice: BigDecimal) {
        var prevReferencePrice = assetPairIdToReferencePrice.getOrPut(assetPair.assetPairId) { BigDecimal.ZERO }

        val result = if (prevReferencePrice == BigDecimal.ZERO || midPrices.size == 0) {
            midPrice
        } else {
            prevReferencePrice += NumberUtils.divideWithMaxScale(midPrice, BigDecimal.valueOf(midPrices.size.toLong()))
            val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(midPrices.size.toLong() + 1), BigDecimal.valueOf(midPrices.size.toLong()))
            NumberUtils.divideWithMaxScale(prevReferencePrice, newMidPriceCoef)
        }

        assetPairIdToReferencePrice[assetPair.assetPairId] = result
    }

    private fun recalculateReferenceMidPriceAfterRemoval(removedMidPricesSum: BigDecimal,
                                                         midSizesInitialSize: Int,
                                                         currentSize: Int,
                                                         assetPair: AssetPair) {
        val removedMidPricesCount = midSizesInitialSize - currentSize

        //we are using previous mid price if no mid prices were removed or new mid price can not be calculated due to no new data
        if (midSizesInitialSize == 0 || removedMidPricesCount == 0 || removedMidPricesCount == midSizesInitialSize) {
            return
        }

        val referencePrice = assetPairIdToReferencePrice.getOrPut(assetPair.assetPairId) { BigDecimal.ZERO }

        //perform full reference mid price recalculation if we exceeded max mid price recalculations count - to
        // prevent accumulation of calculation error
        if (referencePrice == BigDecimal.ZERO || MAX_MID_PRICE_RECALCULATION_COUNT == midPriceRecalculationCount) {
            performFullRecalculationOfReferenceMidPrice(assetPair)
            return
        }

        val midPricesInitialSize = BigDecimal.valueOf(midSizesInitialSize.toLong())
        val referencePriceWithoutObsolete = referencePrice - NumberUtils.divideWithMaxScale(removedMidPricesSum, midPricesInitialSize)
        val newMidPriceCoef = NumberUtils.divideWithMaxScale(midPricesInitialSize - BigDecimal.valueOf(removedMidPricesCount.toLong()), midPricesInitialSize)

        val result = NumberUtils.divideWithMaxScale(referencePriceWithoutObsolete, newMidPriceCoef)
        assetPairIdToReferencePrice[assetPair.assetPairId] = result
        midPriceRecalculationCount++
    }

    private fun performFullRecalculationOfReferenceMidPrice(assetPair: AssetPair) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair.assetPairId) { LinkedList() }

        midPriceRecalculationCount = 0
        var sum = BigDecimal.ZERO
        val midPricesCount = BigDecimal.valueOf(midPrices.size.toLong())

        if (midPricesCount == BigDecimal.ZERO) {
            return
        }

        midPrices.forEach { midPrice -> sum += midPrice.midPrice }
        val result = NumberUtils.divideWithMaxScale(sum, midPricesCount)
        assetPairIdToReferencePrice.put(assetPair.assetPairId, result)
    }

    private fun getLowerTimeBound(operationTimeMillis: Long): Long {
        return operationTimeMillis - refreshMidPricePeriod
    }
}