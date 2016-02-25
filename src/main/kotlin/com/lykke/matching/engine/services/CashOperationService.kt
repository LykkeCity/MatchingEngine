package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger
import java.util.Date
import java.util.HashMap
import java.util.UUID

class CashOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
    }

    private val wallets: HashMap<String, MutableMap<String, Wallet>> = walletDatabaseAccessor.loadWallets()

    override fun processMessage(array: ByteArray) {
        val message = parse(array)
        LOGGER.debug("Processing cash operation for client ${message.accountId}, asset ${message.assetId}")
        val client = wallets.getOrPut(message.accountId) { HashMap<String, Wallet>() }
        val wallet = client.getOrPut(message.assetId) { Wallet(message.accountId, message.assetId) }

        wallet.addBalance(message.amount)

        walletDatabaseAccessor.insertOrUpdateWallet(wallet)
        walletDatabaseAccessor.addOperation(WalletOperation(
                clientId=message.accountId,
                uid=UUID.randomUUID().toString(),
                dateTime= Date(message.date),
                asset=message.assetId,
                amount= message.amount))
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }
}