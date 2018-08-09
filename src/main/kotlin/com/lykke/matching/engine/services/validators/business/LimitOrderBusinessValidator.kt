package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.AssetOrderBook
import java.math.BigDecimal

interface LimitOrderBusinessValidator {
    fun performValidation(isTrustedClient: Boolean,
                          order: LimitOrder,
                          availableBalance: BigDecimal,
                          limitVolume: BigDecimal,
                          orderBook: AssetOrderBook)
    fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal)
}