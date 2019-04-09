package com.lykke.matching.engine.holders

import com.lykke.client.accounts.ClientAccountsCache
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ClientAccountsHolder(val clientAccountsCache: ClientAccountsCache) {
    private companion object {
        val LOGGER = LoggerFactory.getLogger(ClientAccountsHolder::class.java)
    }

    fun getAllWalletsByOperationWalletId(walletId: String): Set<String> {
        val clientId = clientAccountsCache.getClientByWalletId(walletId)

        if (clientId == null) {
            LOGGER.warn("Client was not found by walletId: $walletId")
            return setOf(walletId)
        }

        return clientAccountsCache.getWalletsByClientId(clientId)
    }
}