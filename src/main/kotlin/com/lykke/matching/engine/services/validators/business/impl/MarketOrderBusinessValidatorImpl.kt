package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.validators.business.MarketOrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.springframework.stereotype.Component

@Component
class MarketOrderBusinessValidatorImpl(private val genericLimitOrderService: GenericLimitOrderService) : MarketOrderBusinessValidator {
    override fun performValidation(order: MarketOrder) {
        isOrderBookValid(order)
    }

    private fun isOrderBookValid(order: MarketOrder) {
        if (genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide()).size == 0) {
            throw OrderValidationException(OrderStatus.NoLiquidity)
        }
    }
}