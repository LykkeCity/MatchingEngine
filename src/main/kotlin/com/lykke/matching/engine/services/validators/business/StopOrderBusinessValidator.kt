package com.lykke.matching.engine.services.validators.business

import java.math.BigDecimal

interface StopOrderBusinessValidator {
    fun performValidation(availableBalance: BigDecimal, limitVolume: BigDecimal)
}