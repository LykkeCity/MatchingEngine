package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.order.OrderStatus

class OrderValidationResult(val isValid: Boolean, val message: String? = null, val status: OrderStatus? = null)
