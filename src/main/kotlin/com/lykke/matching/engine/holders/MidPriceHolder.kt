package com.lykke.matching.engine.holders

import com.lykke.matching.engine.common.events.RefMidPriceDangerousChangeEvent
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.monitoring.OrderBookMidPriceChecker
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import java.math.BigDecimal
import java.util.*

@Component
class MidPriceHolder(@Value("#{Config.me.referenceMidPricePeriod}") val refreshMidPricePeriod: Long,
                     readOnlyMidPriceDatabaseAccessor: ReadOnlyMidPriceDatabaseAccessor,
                     private val orderBookMidPriceChecker: OrderBookMidPriceChecker) {
    private val MAX_MID_PRICE_RECALCULATION_COUNT = 1000
    private val midPricesByAssetPairId = HashMap<String, LinkedList<MidPrice>>()
    private val referencePriceByAssetPairId = HashMap<String, BigDecimal>()
    private val prevReferencePriceByAssetPairId = HashMap<String, BigDecimal>()
    private val readyRefMidPricesAssetPairs = HashSet<String>()
    private var midPriceRecalculationCount = 0

    init {
        readOnlyMidPriceDatabaseAccessor
                .getMidPricesByAssetPairMap()
                .forEach { assetPairId, midPrices -> midPricesByAssetPairId[assetPairId] = LinkedList(midPrices) }
        midPricesByAssetPairId.forEach { key, value ->
            performFullRecalculationOfReferenceMidPrice(key)
        }
    }

    fun getReferenceMidPrice(assetPair: AssetPair, executionContext: ExecutionContext): BigDecimal {
        return NumberUtils.setScaleRoundUp(getUnScaledReferenceMidPrice(assetPair, executionContext), assetPair.accuracy)
    }

    fun addMidPrice(assetPair: AssetPair, newMidPrice: BigDecimal, executionContext: ExecutionContext) {
        val operationTime = executionContext.date

        removeObsoleteMidPrices(assetPair.assetPairId, getLowerTimeBound(operationTime.time))
        val midPriceDangerous = isMidPriceChanged(assetPair.assetPairId, newMidPrice) && executionContext.executionContextForCancelOperation
        val midPrices = midPricesByAssetPairId.getOrPut(assetPair.assetPairId) { LinkedList() }
        midPrices.add(MidPrice(assetPair.assetPairId, newMidPrice, operationTime.time))
        recalculateReferenceMidPriceForNewAddedMidPrice(assetPair.assetPairId, newMidPrice)
        if (midPriceDangerous) {
            orderBookMidPriceChecker.checkOrderBook(RefMidPriceDangerousChangeEvent(assetPair.assetPairId, referencePriceByAssetPairId[assetPair.assetPairId]!!, executionContext))
        }
    }

    fun clear() {
        midPricesByAssetPairId.clear()
        referencePriceByAssetPairId.clear()
        prevReferencePriceByAssetPairId.clear()
        readyRefMidPricesAssetPairs.clear()
        midPriceRecalculationCount = 0
    }

    private fun isMidPriceChanged(assetPairId: String, newMidPrice: BigDecimal): Boolean {
        val midPrices = midPricesByAssetPairId[assetPairId]
        if (CollectionUtils.isEmpty(midPrices)) {
            return false
        }
        return !NumberUtils.equalsIgnoreScale(midPrices!!.last.midPrice, newMidPrice)
    }

    private fun getUnScaledReferenceMidPrice(assetPair: AssetPair, executionContext: ExecutionContext): BigDecimal {
        val operationTime = executionContext.date

        val midPriceFirstTimeReady = readyRefMidPricesAssetPairs.add(assetPair.assetPairId)
        removeObsoleteMidPrices(assetPair.assetPairId, getLowerTimeBound(operationTime.time))
        val refMidPrice = referencePriceByAssetPairId[assetPair.assetPairId]

        val result = if (refMidPrice != null && !NumberUtils.equalsIgnoreScale(refMidPrice, BigDecimal.ZERO)) {
            refMidPrice
        } else prevReferencePriceByAssetPairId[assetPair.assetPairId]
                ?: getOrderBookMidPrice(executionContext.orderBooksHolder?.getChangedCopyOrOriginalOrderBook(assetPair.assetPairId))

        if (midPriceFirstTimeReady) {
            orderBookMidPriceChecker.checkOrderBook(RefMidPriceDangerousChangeEvent(assetPair.assetPairId, NumberUtils.setScaleRoundUp(result, assetPair.accuracy), executionContext))
        }

        return result
    }

    private fun getOrderBookMidPrice(assetOrderBook: AssetOrderBook?): BigDecimal {
        return assetOrderBook?.getMidPrice() ?: BigDecimal.ZERO
    }

    private fun removeObsoleteMidPrices(assetPairId: String, lowerBoundTimeMillisBound: Long) {
        val midPrices = midPricesByAssetPairId.getOrPut(assetPairId) { LinkedList() }
        val initialSize = midPrices.size
        var removedMidPricesSum = BigDecimal.ZERO
        while (midPrices.size > 0 && midPrices.first().timestamp < lowerBoundTimeMillisBound) {
            removedMidPricesSum += midPrices.removeFirst().midPrice
        }
        recalculateReferenceMidPriceAfterRemoval(removedMidPricesSum, initialSize, midPrices.size, assetPairId)
    }

    private fun recalculateReferenceMidPriceForNewAddedMidPrice(assetPairId: String,
                                                                midPrice: BigDecimal) {
        if (midPriceRecalculationCount == MAX_MID_PRICE_RECALCULATION_COUNT) {
            performFullRecalculationOfReferenceMidPrice(assetPairId)
            return
        }
        val currentReferenceMidPrice = referencePriceByAssetPairId[assetPairId] ?: BigDecimal.ZERO
        val previousMidPricesSize = midPricesByAssetPairId[assetPairId]!!.size - 1
        val result = if (NumberUtils.equalsIgnoreScale(currentReferenceMidPrice, BigDecimal.ZERO) || previousMidPricesSize == 0) {
            midPrice
        } else {
            val newMidPriceWithOldMidPricesCount = currentReferenceMidPrice + NumberUtils.divideWithMaxScale(midPrice, BigDecimal.valueOf(previousMidPricesSize.toLong()))
            val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(previousMidPricesSize.toLong() + 1), BigDecimal.valueOf(previousMidPricesSize.toLong()))
            NumberUtils.divideWithMaxScale(newMidPriceWithOldMidPricesCount, newMidPriceCoef)
        }
        setRefMidPrice(assetPairId, result)
        midPriceRecalculationCount++
    }

    private fun setRefMidPrice(assetPairId: String, newMidPrice: BigDecimal) {
        val currentRefMidPrice = referencePriceByAssetPairId[assetPairId]
        if (currentRefMidPrice != null && currentRefMidPrice != BigDecimal.ZERO) {
            prevReferencePriceByAssetPairId[assetPairId] = currentRefMidPrice
        }
        referencePriceByAssetPairId[assetPairId] = newMidPrice
    }

    private fun recalculateReferenceMidPriceAfterRemoval(removedMidPricesSum: BigDecimal,
                                                         midPricesInitialSize: Int,
                                                         currentSize: Int,
                                                         assetPairId: String) {
        if (currentSize == 0) {
            setRefMidPrice(assetPairId, BigDecimal.ZERO)
            return
        }
        val removedMidPricesCount = midPricesInitialSize - currentSize
        val referencePrice = referencePriceByAssetPairId.getOrPut(assetPairId) { BigDecimal.ZERO }
        if (midPricesInitialSize == 0 || removedMidPricesCount == 0 || NumberUtils.equalsIgnoreScale(referencePrice, BigDecimal.ZERO)) {
            return
        }
        //perform full reference mid price recalculation if we exceeded max mid price recalculations count - to
        // prevent accumulation of calculation error
        if (MAX_MID_PRICE_RECALCULATION_COUNT == midPriceRecalculationCount) {
            performFullRecalculationOfReferenceMidPrice(assetPairId)
            return
        }
        val midPricesInitialSize = BigDecimal.valueOf(midPricesInitialSize.toLong())
        val referencePriceWithoutObsolete = referencePrice - NumberUtils.divideWithMaxScale(removedMidPricesSum, midPricesInitialSize)
        val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(currentSize.toLong()), midPricesInitialSize)
        val result = NumberUtils.divideWithMaxScale(referencePriceWithoutObsolete, newMidPriceCoef)
        setRefMidPrice(assetPairId, result)
        midPriceRecalculationCount++
    }

    private fun performFullRecalculationOfReferenceMidPrice(assetPairId: String) {
        val midPrices = midPricesByAssetPairId.getOrPut(assetPairId) { LinkedList() }
        midPriceRecalculationCount = 0
        var sum = BigDecimal.ZERO
        val midPricesCount = BigDecimal.valueOf(midPrices.size.toLong())
        if (NumberUtils.equalsIgnoreScale(midPricesCount, BigDecimal.ZERO)) {
            return
        }
        midPrices.forEach { midPrice -> sum += midPrice.midPrice }
        val result = NumberUtils.divideWithMaxScale(sum, midPricesCount)
        setRefMidPrice(assetPairId, result)
    }

    private fun getLowerTimeBound(operationTimeMillis: Long): Long {
        return operationTimeMillis - refreshMidPricePeriod
    }
}