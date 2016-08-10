package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages

class WalletCredentialsCacheService(val walletCredentialsCache: WalletCredentialsCache): AbsractService<ProtocolMessages.WalletCredentialsReload> {

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)

        if (message.hasClientId()) {
            walletCredentialsCache.reloadClient(message.clientId)
        } else {
            walletCredentialsCache.reloadCache()
        }
    }

    private fun parse(array: ByteArray): ProtocolMessages.WalletCredentialsReload {
        return ProtocolMessages.WalletCredentialsReload.parseFrom(array)
    }
}