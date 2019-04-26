package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.business.OrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class OrderBusinessValidatorImpl: OrderBusinessValidator {
    override fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "not enough funds to reserve")
        }
    }

    override fun validateExpiration(order: LimitOrder, orderProcessingTime: Date) {
        if (order.isExpired(orderProcessingTime)) {
            throw OrderValidationException(OrderStatus.Cancelled, "expired")
        }
    }
}