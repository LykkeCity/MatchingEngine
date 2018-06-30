package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor

class AzureCashOperationIdDatabaseAccessor: CashOperationIdDatabaseAccessor {
    override fun isAlreadyProcessed(type: String, id: String): Boolean {
        return false
    }
}