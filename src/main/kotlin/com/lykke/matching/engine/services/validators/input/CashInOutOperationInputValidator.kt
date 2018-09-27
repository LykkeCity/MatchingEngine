package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData

interface CashInOutOperationInputValidator {
    fun performValidation(cashInOutParsedData: CashInOutParsedData)
}