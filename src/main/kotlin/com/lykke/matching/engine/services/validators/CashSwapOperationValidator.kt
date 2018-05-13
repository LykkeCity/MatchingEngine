package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface CashSwapOperationValidator {
    fun performValidation(message: ProtocolMessages.CashSwapOperation, operationId: String)
}