package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.order.OrderStatus
import org.apache.commons.lang3.StringUtils

class OrderValidationException(val orderStatus: OrderStatus, message: String = StringUtils.EMPTY) : MatchingEngineException(message)