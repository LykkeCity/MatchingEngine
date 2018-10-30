package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class StopOrderBusinessValidatorImpl: StopOrderBusinessValidator {
    override fun performValidation(availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   order: LimitOrder,
                                   orderProcessingTime: Date) {
        OrderValidationUtils.validateBalance(availableBalance, limitVolume)
        OrderValidationUtils.validateExpiration(order, orderProcessingTime)
    }
}