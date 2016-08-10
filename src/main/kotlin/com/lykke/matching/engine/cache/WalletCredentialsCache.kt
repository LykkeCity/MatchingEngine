package com.lykke.matching.engine.cache

import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor

class WalletCredentialsCache {

    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor

    private var wallets: MutableMap<String, WalletCredentials>

    constructor(backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor) {
        this.backOfficeDatabaseAccessor = backOfficeDatabaseAccessor
        wallets = this.backOfficeDatabaseAccessor.loadAllWalletCredentials()
    }

    fun getWalletCredentials(clientId: String): WalletCredentials? {
        if (wallets.containsKey(clientId)) {
            return wallets[clientId]
        }
        val wallet = backOfficeDatabaseAccessor.loadWalletCredentials(clientId)
        if (wallet != null) {
            wallets[clientId] = wallet
        }

        return wallet
    }

    fun reloadCache() {
        this.wallets = backOfficeDatabaseAccessor.loadAllWalletCredentials()
    }

    fun reloadClient(clientId: String) {
        wallets[clientId] = backOfficeDatabaseAccessor.loadWalletCredentials(clientId)!!
    }
}