package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.common.entity.PersistenceData
import java.util.*

class TestPersistenceManager(private val walletDatabaseAccessor: WalletDatabaseAccessor): PersistenceManager {

    var persistenceErrorMode = false

    override fun persist(data: PersistenceData): Boolean {
        if (persistenceErrorMode) {
            return false
        }
        if (data.balancesData != null) {
            walletDatabaseAccessor.insertOrUpdateWallets(ArrayList(data.balancesData?.wallets))
        }
        return true
    }

    override fun balancesQueueSize() = 0
}