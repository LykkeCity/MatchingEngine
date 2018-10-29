package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.services.validators.business.LimitOrderCancelOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import org.springframework.stereotype.Component

@Component
class LimitOrderCancelOperationBusinessValidatorImpl : LimitOrderCancelOperationBusinessValidator {
    override fun performValidation(typeToOrder: Map<LimitOrderType, List<LimitOrder>>, context: LimitOrderCancelOperationContext) {
        validateOrdersAreFound(typeToOrder, context)
    }

    private fun validateOrdersAreFound(typeToOrder: Map<LimitOrderType, List<LimitOrder>>, context: LimitOrderCancelOperationContext) {
        if (typeToOrder.isEmpty()) {
            throw ValidationException(ValidationException.Validation.LIMIT_ORDER_NOT_FOUND, "Unable to find order ids: ${context.limitOrderIds}")
        }
    }
}