package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.LimitOrder
import java.math.BigDecimal
import java.util.*

interface OrderBusinessValidator {
    fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal)
    fun validateExpiration(order: LimitOrder, orderProcessingTime: Date)
}