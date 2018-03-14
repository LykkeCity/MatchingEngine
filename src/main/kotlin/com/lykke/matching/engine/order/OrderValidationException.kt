package com.lykke.matching.engine.order

import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.messages.MessageStatus

class OrderValidationException(message: String,
                               val orderStatus: OrderStatus,
                               val messageStatus: MessageStatus) : MatchingEngineException(message)