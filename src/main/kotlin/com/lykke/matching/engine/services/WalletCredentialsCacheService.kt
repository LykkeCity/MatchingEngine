package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger

class WalletCredentialsCacheService(val walletCredentialsCache: WalletCredentialsCache): AbstractService {

    companion object {
        val LOGGER = Logger.getLogger(WalletCredentialsCacheService::class.java)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got WalletCredentialsCache reset request: clientId=${message.clientId}")

        if (message.hasClientId()) {
            walletCredentialsCache.reloadClient(message.clientId)
        } else {
            walletCredentialsCache.reloadCache()
        }
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        LOGGER.debug("WalletCredentialsCache reset request: clientId=${message.clientId} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.WalletCredentialsReload {
        return ProtocolMessages.WalletCredentialsReload.parseFrom(array)
    }
}