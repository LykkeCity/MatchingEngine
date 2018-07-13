package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.context.CashTransferContext

interface CashTransferOperationBusinessValidator {
    fun performValidation(cashTransferContext: CashTransferContext)
}