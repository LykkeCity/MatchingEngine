package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.validators.business.OrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class StopOrderBusinessValidatorImpl(private val orderBusinessValidator: OrderBusinessValidator): StopOrderBusinessValidator {
    override fun performValidation(availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   order: LimitOrder,
                                   orderProcessingTime: Date) {
        orderBusinessValidator.validateBalance(availableBalance, limitVolume)
        orderBusinessValidator.validateExpiration(order, orderProcessingTime)
    }
}