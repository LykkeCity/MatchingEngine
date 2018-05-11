package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface CashSwapOperationValidator {
    fun performValidation(cashSwapOperation: ProtocolMessages.CashSwapOperation, operationId: String)
}