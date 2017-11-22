package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation

interface CashOperationsDatabaseAccessor {
    fun insertExternalCashOperation(operation: ExternalCashOperation)
    fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation?
    fun insertOperation(operation: WalletOperation)
    fun insertTransferOperation(operation: TransferOperation)
    fun insertSwapOperation(operation: SwapOperation)
}