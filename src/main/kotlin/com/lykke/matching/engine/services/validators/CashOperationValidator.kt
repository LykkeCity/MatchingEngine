package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface CashOperationValidator {
    fun performValidation(cashOperation: ProtocolMessages.CashOperation)
}