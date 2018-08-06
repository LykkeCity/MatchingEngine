package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData

interface LimitOrderCancelOperationValidator {
    fun performValidation(limitOrderCancelOperationParsedData: LimitOrderCancelOperationParsedData)
}