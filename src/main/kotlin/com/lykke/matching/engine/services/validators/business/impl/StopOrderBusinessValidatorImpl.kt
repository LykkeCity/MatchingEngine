package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolder
import com.lykke.matching.engine.services.validators.business.OrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.Date

@Component
class StopOrderBusinessValidatorImpl(private val orderBookMaxTotalSizeHolder: OrderBookMaxTotalSizeHolder,
                                     private val orderBusinessValidator: OrderBusinessValidator)
    : StopOrderBusinessValidator {
    override fun performValidation(availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   order: LimitOrder,
                                   orderProcessingTime: Date,
                                   currentOrderBookTotalSize: Int) {
        OrderValidationUtils.validateOrderBookTotalSize(currentOrderBookTotalSize, orderBookMaxTotalSizeHolder.get())
        orderBusinessValidator.validateBalance(availableBalance, limitVolume)
        orderBusinessValidator.validateExpiration(order, orderProcessingTime)
    }
}