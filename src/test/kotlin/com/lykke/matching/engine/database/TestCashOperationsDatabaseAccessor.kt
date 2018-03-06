package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import java.util.LinkedList

class TestCashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor {

    private val transferOperations = LinkedList<TransferOperation>()
    private val swapOperations = LinkedList<SwapOperation>()

    override fun insertTransferOperation(operation: TransferOperation) {
        this.transferOperations.add(operation)
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        this.swapOperations.add(operation)
    }

    fun clear() {
        transferOperations.clear()
        swapOperations.clear()
    }
}