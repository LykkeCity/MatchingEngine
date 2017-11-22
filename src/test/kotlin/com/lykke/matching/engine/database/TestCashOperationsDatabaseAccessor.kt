package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import java.util.LinkedList

class TestCashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor {

    private val operations = LinkedList<WalletOperation>()
    private val transferOperations = LinkedList<TransferOperation>()
    private val externalOperations = LinkedList<ExternalCashOperation>()

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        return externalOperations.find { it.clientId == clientId && it.externalId == operationId }
    }

    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        this.externalOperations.add(operation)
    }

    override fun insertOperation(operation: WalletOperation) {
        this.operations.add(operation)
    }
    override fun insertTransferOperation(operation: TransferOperation) {
        this.transferOperations.add(operation)
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun clear() {
        operations.clear()
        transferOperations.clear()
        externalOperations.clear()
    }
}