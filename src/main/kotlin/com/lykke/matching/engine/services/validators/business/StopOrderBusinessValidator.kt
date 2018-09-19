package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.LimitOrder
import java.math.BigDecimal
import java.util.*

interface StopOrderBusinessValidator {
    fun performValidation(availableBalance: BigDecimal,
                          limitVolume: BigDecimal,
                          order: LimitOrder, orderProcessingTime: Date)
}