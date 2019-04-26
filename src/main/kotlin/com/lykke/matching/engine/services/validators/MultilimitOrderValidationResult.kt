package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.services.validators.impl.OrderValidationResult

data class MultilimitOrderValidationResult (val globalValidationResult: OrderValidationResult,
                                            val inputValidationResultByOrderId: Map<String, OrderValidationResult>? = null)