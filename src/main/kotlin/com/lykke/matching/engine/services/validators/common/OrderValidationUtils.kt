package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException

class OrderValidationUtils {
    companion object {
        fun isFatalInvalid(validationException: OrderValidationException): Boolean {
            return validationException.orderStatus == OrderStatus.UnknownAsset
        }
    }
}