package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.PersistenceData

class TestPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor): PersistenceManager {

    override fun persist(data: PersistenceData) {
        walletDatabaseAccessor.insertOrUpdateWallets(data.wallets.toList())
    }

    override fun balancesQueueSize() = 0
}