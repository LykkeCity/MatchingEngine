package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.Date

@Component
class LimitOrderBusinessValidatorImpl: LimitOrderBusinessValidator {
    override fun performValidation(isTrustedClient: Boolean, order: LimitOrder,
                                   availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   orderBook: AssetOrderBook,
                                   date: Date) {

        if (!isTrustedClient) {
            validateBalance(availableBalance, limitVolume)
        }

        leadToNegativeSpread(order, orderBook)
        previousOrderFound(order)
        enoughFunds(order)
        checkExpiration(order, date)
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

    private fun enoughFunds(order: LimitOrder) {
        if (order.status == OrderStatus.NotEnoughFunds.name) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "${orderInfo(order)} has not enough funds")
        }
    }

    private fun leadToNegativeSpread(order: LimitOrder, orderBook: AssetOrderBook) {
        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread, "Limit order (id: ${order.externalId}) lead to negative spread")
        }
    }

    override fun checkExpiration(order: LimitOrder, date: Date) {
        if (order.isExpired(date)) {
            throw OrderValidationException(OrderStatus.Cancelled, "expired")
        }
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"
}