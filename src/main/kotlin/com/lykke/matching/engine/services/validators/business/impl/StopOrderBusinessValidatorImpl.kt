package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class StopOrderBusinessValidatorImpl: StopOrderBusinessValidator {
    override fun performValidation(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        OrderValidationUtils.validateBalance(availableBalance, limitVolume)
    }
}