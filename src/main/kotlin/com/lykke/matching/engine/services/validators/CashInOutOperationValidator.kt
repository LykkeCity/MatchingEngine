package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface CashInOutOperationValidator {
    fun performValidation(cashInOutOperation: ProtocolMessages.CashInOutOperation)
}