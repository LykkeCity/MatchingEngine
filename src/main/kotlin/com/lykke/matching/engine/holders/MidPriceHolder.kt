package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.ReadOnlyMidPriceDatabaseAccessor
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import javax.annotation.PostConstruct

@Component
class MidPriceHolder(@Value("#{Config.me.referenceMidPricePeriod}") private val refreshMidPricePeriod: Long,
                     private val readOnlyMidPriceDatabaseAccessor: ReadOnlyMidPriceDatabaseAccessor) {

    private val MAX_MID_PRICE_RECALCULATION_COUNT = 1000

    private var midPrices = LinkedList<MidPrice>()
    private lateinit var referenceMidPrice: BigDecimal
    private var midPriceRecalculationCount = 0

    @PostConstruct
    private fun init() {
        midPrices = LinkedList(readOnlyMidPriceDatabaseAccessor.all())
        performFullRecalculationOfReferenceMidPrice()
    }

    fun getReferenceMidPrice(operationTime: Date): BigDecimal {
        removeObsoleteMidPrices(getLowerTimeBound(operationTime.time))

        return referenceMidPrice
    }

    fun addMidPrice(midPrice: BigDecimal, operationTime: Date) {
        removeObsoleteMidPrices(getLowerTimeBound(operationTime.time))
        midPrices.add(MidPrice(midPrice, operationTime.time))

        referenceMidPrice += NumberUtils.divideWithMaxScale(midPrice, BigDecimal.valueOf(midPrices.size.toLong()))
        val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(midPrices.size.toLong()), BigDecimal.valueOf(midPrices.size.toLong() - 1))
        referenceMidPrice = NumberUtils.divideWithMaxScale(referenceMidPrice, newMidPriceCoef)
    }

    private fun removeObsoleteMidPrices(lowerBoundTimeMillisBound: Long) {
        val initialSize = midPrices.size
        var removedMidPricesSum = BigDecimal.ZERO

        while (midPrices.size > 0 && midPrices.first().timestamp < lowerBoundTimeMillisBound) {
            removedMidPricesSum += midPrices.removeFirst().midPrice
        }

        calculateCurrentReferenceMidPrice(removedMidPricesSum, initialSize)
    }

    private fun calculateCurrentReferenceMidPrice(removedMidPricesSum: BigDecimal, midSizesInitialSize: Int) {
        val removedMidPricesCount = midSizesInitialSize - midPrices.size

        //we are using previous mid price if no mid prices were removed or new mid price can not be calculated due to no new data
        if (removedMidPricesCount == 0 || removedMidPricesCount == midSizesInitialSize) {
            return
        }

        //perform full mid price recalculation if prev mid prices empty or we exceeded max mid price recalculations - to
        // prevent accumulation of calculation error
        if (midSizesInitialSize == 0 || MAX_MID_PRICE_RECALCULATION_COUNT == midPriceRecalculationCount) {
            performFullRecalculationOfReferenceMidPrice()
            return
        }

        val referencePriceWithoutObsolete = referenceMidPrice - NumberUtils.divideWithMaxScale(removedMidPricesSum, BigDecimal.valueOf(midSizesInitialSize.toLong()))
        val newMidPriceCoef = NumberUtils.divideWithMaxScale(BigDecimal.valueOf(removedMidPricesCount.toLong()), BigDecimal.valueOf(midSizesInitialSize.toLong()))

        referenceMidPrice = NumberUtils.divideWithMaxScale(referencePriceWithoutObsolete, newMidPriceCoef)
        midPriceRecalculationCount++
    }

    private fun performFullRecalculationOfReferenceMidPrice() {
        midPriceRecalculationCount = 0
        var sum = BigDecimal.ZERO
        val midPricesCount = BigDecimal.valueOf(midPrices.size.toLong())

        if (midPricesCount == BigDecimal.ZERO) {
            referenceMidPrice = BigDecimal.ZERO
            return
        }

        midPrices.forEach { midPrice -> sum += midPrice.midPrice }
        referenceMidPrice = NumberUtils.divideWithMaxScale(sum, midPricesCount)
    }

    private fun getLowerTimeBound(operationTimeMillis: Long): Long {
        return operationTimeMillis - refreshMidPricePeriod
    }
}