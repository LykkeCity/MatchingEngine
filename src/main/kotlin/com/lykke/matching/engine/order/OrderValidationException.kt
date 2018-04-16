package com.lykke.matching.engine.order

import com.lykke.matching.engine.exception.MatchingEngineException

class OrderValidationException(message: String,
                               val orderStatus: OrderStatus) : MatchingEngineException(message)