package com.lykke.matching.engine.order

import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.utils.order.OrderStatusUtils

class OrderValidationException(message: String,
                               val orderStatus: OrderStatus) : MatchingEngineException(message) {
    val messageStatus = OrderStatusUtils.toMessageStatus(orderStatus)
}