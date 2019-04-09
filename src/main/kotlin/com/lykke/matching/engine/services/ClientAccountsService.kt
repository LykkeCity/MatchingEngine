package com.lykke.matching.engine.services

interface ClientAccountsService {
    fun getAllWalletsByOperationWalletId(walletId: String): Set<String>
}