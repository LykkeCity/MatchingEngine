package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType

interface LimitOrderCancelOperationBusinessValidator {
    fun performValidation(typeToOrder: Map<LimitOrderType, List<LimitOrder>>, context: LimitOrderCancelOperationContext)
}