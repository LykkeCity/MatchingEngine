package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.context.CashOperationContext

interface CashOperationBusinessValidator {
    fun performValidation(cashOperationContext: CashOperationContext)
}