package com.lykke.matching.engine.order

import com.lykke.matching.engine.exception.MatchingEngineException
import org.apache.commons.lang3.StringUtils

class OrderValidationException(val orderStatus: OrderStatus, message: String = StringUtils.EMPTY) : MatchingEngineException(message)