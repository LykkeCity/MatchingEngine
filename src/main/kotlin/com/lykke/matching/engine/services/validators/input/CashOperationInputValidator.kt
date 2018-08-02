package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData

interface CashOperationInputValidator {
    fun performValidation(cashOperationParsedData: CashOperationParsedData)
}