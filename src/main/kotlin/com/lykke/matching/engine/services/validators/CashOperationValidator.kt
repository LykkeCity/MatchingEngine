package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData

interface CashOperationValidator {
    fun performValidation(cashOperation: CashOperationParsedData)
}