package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType.WALLET_CREDENTIALS_RELOAD
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.Date
import kotlin.test.assertEquals

class WalletCredentialsCacheServiceTest {
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAcessor)

    @Before
    fun setUp() {

        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client1", "Client1-Multisig"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client2", "Client2-Multisig"))

        this.walletCredentialsCache.reloadCache()
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testClientReload() {
        val service = WalletCredentialsCacheService(walletCredentialsCache)

        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client1", "Client1-Multisig-New"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client2", "Client2-Multisig-New"))

        service.processMessage(MessageWrapper(WALLET_CREDENTIALS_RELOAD.type, ProtocolMessages.WalletCredentialsReload.newBuilder().setUid(Date().time).setClientId("Client1").build().toByteArray(), null))

        assertEquals("Client1-Multisig-New", service.walletCredentialsCache.getWalletCredentials("Client1")!!.multiSig)
        assertEquals("Client2-Multisig", service.walletCredentialsCache.getWalletCredentials("Client2")!!.multiSig)
    }

    @Test
    fun testClientReloadAll() {
        val service = WalletCredentialsCacheService(walletCredentialsCache)

        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client1", "Client1-Multisig-New"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Wallet", "Client2", "Client2-Multisig-New"))

        service.processMessage(MessageWrapper(WALLET_CREDENTIALS_RELOAD.type, ProtocolMessages.WalletCredentialsReload.newBuilder().setUid(Date().time).build().toByteArray(), null))

        assertEquals("Client1-Multisig-New", service.walletCredentialsCache.getWalletCredentials("Client1")!!.multiSig)
        assertEquals("Client2-Multisig-New", service.walletCredentialsCache.getWalletCredentials("Client2")!!.multiSig)
    }
}