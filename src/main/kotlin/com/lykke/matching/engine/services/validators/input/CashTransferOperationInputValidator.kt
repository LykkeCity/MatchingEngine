package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData

interface CashTransferOperationInputValidator {
    fun performValidation(cashTransferParsedData: CashTransferParsedData)
}