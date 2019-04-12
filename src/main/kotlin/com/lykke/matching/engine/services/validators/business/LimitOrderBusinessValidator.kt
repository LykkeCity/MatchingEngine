package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.AssetOrderBook
import java.math.BigDecimal
import java.util.*

interface LimitOrderBusinessValidator {
    fun performValidation(isTrustedClient: Boolean,
                          order: LimitOrder,
                          availableBalance: BigDecimal,
                          limitVolume: BigDecimal,
                          assetPair: AssetPair,
                          orderBook: AssetOrderBook,
                          date: Date)
}