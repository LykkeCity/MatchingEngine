package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.context.CashInOutContext

interface CashInOutOperationValidator {
    fun performValidation(cashInOutContext: CashInOutContext)
}