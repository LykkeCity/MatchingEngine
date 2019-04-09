package com.lykke.matching.engine.services

import com.lykke.client.accounts.ClientAccountsCache
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ClientAccountsServiceImpl(val clientAccountsCache: ClientAccountsCache): ClientAccountsService {

    private companion object {
        val LOGGER = LoggerFactory.getLogger(ClientAccountsServiceImpl::class.java)
    }

    override fun getAllWalletsByOperationWalletId(walletId: String): Set<String> {
        val clientId = clientAccountsCache.getClientByWalletId(walletId)

        if (clientId == null) {
            LOGGER.warn("Client was not found by walletId: $walletId")
            return setOf(walletId)
        }

        return clientAccountsCache.getWalletsByClientId(clientId)
    }
}