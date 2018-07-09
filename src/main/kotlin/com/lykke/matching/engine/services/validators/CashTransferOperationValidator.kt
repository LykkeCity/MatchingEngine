package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.context.CashTransferContext

interface CashTransferOperationValidator {
    fun performValidation(cashTransferContext: CashTransferContext)
}