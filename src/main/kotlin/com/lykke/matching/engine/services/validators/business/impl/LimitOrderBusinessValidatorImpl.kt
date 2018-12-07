package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class LimitOrderBusinessValidatorImpl: LimitOrderBusinessValidator {
    override fun performValidation(isTrustedClient: Boolean, order: LimitOrder,
                                   availableBalance: BigDecimal,
                                   limitVolume: BigDecimal,
                                   orderBook: AssetOrderBook) {

        if (!isTrustedClient) {
            OrderValidationUtils.validateBalance(availableBalance, limitVolume)
        }

        validatePreviousOrderNotFound(order)
        validateNotEnoughFounds(order)
    }

    private fun validatePreviousOrderNotFound(order: LimitOrder) {
        if (order.status == OrderStatus.NotFoundPrevious.name) {
            throw OrderValidationException(OrderStatus.NotFoundPrevious, "${orderInfo(order)} has not found previous order (${order.previousExternalId})")
        }
    }

    private fun validateNotEnoughFounds(order: LimitOrder) {
        if (order.status == OrderStatus.NotEnoughFunds.name) {
            throw OrderValidationException(OrderStatus.NotEnoughFunds, "${orderInfo(order)} has not enough funds")
        }
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"
}