package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.order.OrderStatus

data class OrderValidationResult(val isValid: Boolean,
                            val isFatalInvalid: Boolean = false,
                            val message: String? = null,
                            val status: OrderStatus? = null)
