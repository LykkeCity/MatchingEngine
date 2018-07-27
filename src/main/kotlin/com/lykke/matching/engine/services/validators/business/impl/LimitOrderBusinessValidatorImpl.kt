package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class LimitOrderBusinessValidatorImpl: LimitOrderBusinessValidator {
    override fun performValidation(isTrustedClient: Boolean, order: LimitOrder,
                                   availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   orderBook: AssetOrderBook) {

        if (!isTrustedClient) {
            validateBalance(availableBalance, limitVolume)
        }

        leadToNegativeSpread(order, orderBook)
        previousOrderFound(order)
    }

    override fun validateBalance(availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (availableBalance < limitVolume) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "not enough funds to reserve")
        }
    }

    private fun previousOrderFound(order: LimitOrder) {
        if (order.status == OrderStatus.NotFoundPrevious.name) {
            throw OrderValidationException(OrderStatus.NotFoundPrevious, "${orderInfo(order)} has not found previous order (${order.previousExternalId})")
        }
    }

    private fun leadToNegativeSpread(order: LimitOrder, orderBook: AssetOrderBook) {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread, "Limit order (id: ${order.externalId}) lead to negative spread")
        }
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"
}