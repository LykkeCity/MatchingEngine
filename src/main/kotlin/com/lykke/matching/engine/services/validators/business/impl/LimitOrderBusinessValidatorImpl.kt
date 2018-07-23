package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import java.math.BigDecimal

class LimitOrderBusinessValidatorImpl: LimitOrderBusinessValidator {
    override fun performValidation(availableBalance: BigDecimal, limitVolume: BigDecimal, order: LimitOrder, orderBook: AssetOrderBook) {
        validateBalance(availableBalance, limitVolume)
        leadToNegativeSpread(order, orderBook)
    }

    fun previousOrderFound(order: LimitOrder) {
        if (order.status == OrderStatus.NotFoundPrevious.name) {
            throw OrderValidationException(OrderStatus.NotFoundPrevious, "${orderInfo(order)} has not found previous order (${order.previousExternalId})")
        }
    }

    fun leadToNegativeSpread(order: LimitOrder, orderBook: AssetOrderBook) {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread, "Limit order (id: ${order.externalId}) lead to negative spread")
        }
    }

    override fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "not enough funds to reserve")
        }
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"
}