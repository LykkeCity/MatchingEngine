package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.PersistenceData

class TestPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor): PersistenceManager {

    var persistenceErrorMode = false

    override fun persist(data: PersistenceData): Boolean {
        if (persistenceErrorMode) {
            return false
        }
        walletDatabaseAccessor.insertOrUpdateWallets(data.wallets.toList())
        return true
    }

    override fun balancesQueueSize() = 0
}