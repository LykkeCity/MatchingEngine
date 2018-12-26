package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.order.transaction.ExecutionContext
import java.math.BigDecimal

interface MidPriceHolder {
    fun getReferenceMidPrice(assetPair: AssetPair,
                             executionContext: ExecutionContext,
                             notSavedMidPricesSum: BigDecimal? = null,
                             notSavedMidPricesLength: Int? = null): BigDecimal


    fun addMidPrice(assetPair: AssetPair, newMidPrice: BigDecimal, executionContext: ExecutionContext)

    fun clear()
}