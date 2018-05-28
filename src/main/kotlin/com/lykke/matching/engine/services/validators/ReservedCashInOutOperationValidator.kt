package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface ReservedCashInOutOperationValidator {
    fun performValidation(message: ProtocolMessages.ReservedCashInOutOperation)
}