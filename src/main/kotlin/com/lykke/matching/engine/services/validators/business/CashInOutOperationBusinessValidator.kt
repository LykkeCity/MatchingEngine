package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.context.CashInOutContext

interface CashInOutOperationBusinessValidator {
    fun performValidation(cashInOutContext: CashInOutContext)
}