package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface CashTransferOperationValidator {
    fun performValidation(cashTransferOperation: ProtocolMessages.CashTransferOperation, operationId: String)
}