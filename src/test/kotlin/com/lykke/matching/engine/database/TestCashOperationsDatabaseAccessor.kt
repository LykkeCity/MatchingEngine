package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import java.util.LinkedList

class TestCashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor {

    private val transferOperations = LinkedList<TransferOperation>()

    override fun insertTransferOperation(operation: TransferOperation) {
        this.transferOperations.add(operation)
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun clear() {
        transferOperations.clear()
    }
}