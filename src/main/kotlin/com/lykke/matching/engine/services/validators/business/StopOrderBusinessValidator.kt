package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.AssetOrderBook
import java.math.BigDecimal
import java.util.*

interface StopOrderBusinessValidator {
    fun performValidation(availableBalance: BigDecimal,
                          limitVolume: BigDecimal,
                          order: LimitOrder,
                          orderProcessingTime: Date,
                          assetPair: AssetPair,
                          orderBook: AssetOrderBook,
                          currentOrderBookTotalSize: Int)
}