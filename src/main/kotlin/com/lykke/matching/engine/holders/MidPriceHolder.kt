package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class MidPriceHolder(@Value("#{Config.me.referenceMidPricePeriod}") private val refreshMidPricePeriod: Long,
                     readOnlyMidPriceDatabaseAccessor: ReadOnlyMidPriceDatabaseAccessor,
                     assetsPairsHolder: AssetsPairsHolder) {

    private val MAX_MID_PRICE_RECALCULATION_COUNT = 1000

    private val assetPairIdToMidPrices = HashMap<String, LinkedList<MidPrice>>()
    private val assetPairIdToReferencePrice = HashMap<String, BigDecimal>()
    private val assetPairIdToPrevReferencePrice = HashMap<String, BigDecimal>()
    private val assetPairIdToFirstMidPriceTimestamp = HashMap<String, Long>()

    private var midPriceRecalculationCount = 0


    init {
        readOnlyMidPriceDatabaseAccessor
                .getMidPricesByAssetPairMap()
                .forEach { assetPairId, midPrices -> assetPairIdToMidPrices[assetPairId] = LinkedList(midPrices) }

        assetPairIdToMidPrices.forEach { key, value ->
            val assetPair = assetsPairsHolder.getAssetPairAllowNulls(key)
            assetPairIdToFirstMidPriceTimestamp[key] = value.first.timestamp
            if (assetPair != null) performFullRecalculationOfReferenceMidPrice(assetPair)
        }
    }

    fun getReferenceMidPrice(assetPair: AssetPair, operationTime: Date): BigDecimal {
        if (!isMidPriceDataReady(assetPair)) {
            return BigDecimal.ZERO
        }

        removeObsoleteMidPrices(assetPair, getLowerTimeBound(operationTime.time))

        val refMidPrice = assetPairIdToReferencePrice[assetPair.assetPairId]

        val result = if (refMidPrice != null && !NumberUtils.equalsIgnoreScale(refMidPrice, BigDecimal.ZERO)) {
            refMidPrice
        } else assetPairIdToPrevReferencePrice[assetPair.assetPairId]
        return result?.let { NumberUtils.setScaleRoundUp(result, assetPair.accuracy) } ?: BigDecimal.ZERO
    }

    fun addMidPrice(assetPair: AssetPair, newMidPrice: BigDecimal, operationTime: Date) {
        assetPairIdToFirstMidPriceTimestamp.putIfAbsent(assetPair.assetPairId, operationTime.time)
        removeObsoleteMidPrices(assetPair, getLowerTimeBound(operationTime.time))

        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair.assetPairId) { LinkedList() }

        midPrices.add(MidPrice(assetPair.assetPairId, newMidPrice, operationTime.time))
        recalculateReferenceMidPriceForNewAddedMidPrice(assetPair, newMidPrice)
    }

    fun clear() {
        assetPairIdToFirstMidPriceTimestamp.clear()
        assetPairIdToMidPrices.clear()
        assetPairIdToReferencePrice.clear()
        assetPairIdToPrevReferencePrice.clear()
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
                                                                midPrice: BigDecimal) {

        if (midPriceRecalculationCount == MAX_MID_PRICE_RECALCULATION_COUNT) {
            performFullRecalculationOfReferenceMidPrice(assetPair)
            return
        }

        val currentReferenceMidPrice = assetPairIdToReferencePrice[assetPair.assetPairId] ?: BigDecimal.ZERO
        val previousMidPricesSize = assetPairIdToMidPrices[assetPair.assetPairId]!!.size - 1

        val result = if (NumberUtils.equalsIgnoreScale(currentReferenceMidPrice, BigDecimal.ZERO) || previousMidPricesSize == 0) {
            midPrice
        } else {
            val newMidPriceWithOldMidPricesCount = currentReferenceMidPrice + NumberUtils.divideWithMaxScale(midPrice, BigDecimal.valueOf(previousMidPricesSize.toLong()))
            val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(previousMidPricesSize.toLong() + 1), BigDecimal.valueOf(previousMidPricesSize.toLong()))
            NumberUtils.divideWithMaxScale(newMidPriceWithOldMidPricesCount, newMidPriceCoef)
        }

        setRefMidPrice(assetPair, result)
        midPriceRecalculationCount++
    }

    private fun setRefMidPrice(assetPair: AssetPair, newMidPrice: BigDecimal) {
        assetPairIdToReferencePrice[assetPair.assetPairId]?.let {
            assetPairIdToPrevReferencePrice[assetPair.assetPairId] = it
        }

        assetPairIdToReferencePrice[assetPair.assetPairId] = newMidPrice
    }

    private fun recalculateReferenceMidPriceAfterRemoval(removedMidPricesSum: BigDecimal,
                                                         midPricesInitialSize: Int,
                                                         currentSize: Int,
                                                         assetPair: AssetPair) {
        if (currentSize == 0) {
            setRefMidPrice(assetPair, BigDecimal.ZERO)
            return
        }

        val removedMidPricesCount = midPricesInitialSize - currentSize
        val referencePrice = assetPairIdToReferencePrice.getOrPut(assetPair.assetPairId) { BigDecimal.ZERO }

        if (midPricesInitialSize == 0 || removedMidPricesCount == 0 || NumberUtils.equalsIgnoreScale(referencePrice, BigDecimal.ZERO)) {
            return
        }

        //perform full reference mid price recalculation if we exceeded max mid price recalculations count - to
        // prevent accumulation of calculation error
        if (MAX_MID_PRICE_RECALCULATION_COUNT == midPriceRecalculationCount) {
            performFullRecalculationOfReferenceMidPrice(assetPair)
            return
        }

        val midPricesInitialSize = BigDecimal.valueOf(midPricesInitialSize.toLong())
        val referencePriceWithoutObsolete = referencePrice - NumberUtils.divideWithMaxScale(removedMidPricesSum, midPricesInitialSize)
        val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(currentSize.toLong()), midPricesInitialSize)

        val result = NumberUtils.divideWithMaxScale(referencePriceWithoutObsolete, newMidPriceCoef)
        setRefMidPrice(assetPair, result)
        midPriceRecalculationCount++
    }

    private fun performFullRecalculationOfReferenceMidPrice(assetPair: AssetPair) {
        val midPrices = assetPairIdToMidPrices.getOrPut(assetPair.assetPairId) { LinkedList() }

        midPriceRecalculationCount = 0
        var sum = BigDecimal.ZERO
        val midPricesCount = BigDecimal.valueOf(midPrices.size.toLong())

        if (NumberUtils.equalsIgnoreScale(midPricesCount, BigDecimal.ZERO)) {
            return
        }

        midPrices.forEach { midPrice -> sum += midPrice.midPrice }
        val result = NumberUtils.divideWithMaxScale(sum, midPricesCount)
        setRefMidPrice(assetPair, result)
    }

    private fun getLowerTimeBound(operationTimeMillis: Long): Long {
        return operationTimeMillis - refreshMidPricePeriod
    }

    private fun isMidPriceDataReady(assetPair: AssetPair): Boolean {
        val timestamp = assetPairIdToFirstMidPriceTimestamp[assetPair.assetPairId] ?: return false
        return timestamp + refreshMidPricePeriod <= Date().time
    }
}