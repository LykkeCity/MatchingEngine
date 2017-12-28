package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation

interface CashOperationsDatabaseAccessor {
    fun insertTransferOperation(operation: TransferOperation)
    fun insertSwapOperation(operation: SwapOperation)
}