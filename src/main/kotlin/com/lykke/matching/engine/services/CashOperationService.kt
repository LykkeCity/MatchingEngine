package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.apache.log4j.Logger
import java.util.Date
import java.util.HashMap
import java.util.LinkedList

class CashOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
    }

    private val balances = walletDatabaseAccessor.loadBalances()
    private val wallets = walletDatabaseAccessor.loadWallets()
    private val assetPairs = walletDatabaseAccessor.loadAssetPairs()

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash operation for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount}")
        val operation = WalletOperation(
                clientId=message.clientId,
                uid=message.uid.toString(),
                dateTime= Date(message.dateTime),
                asset=message.assetId,
                amount= message.amount)
        processWalletOperations(listOf(operation))
        walletDatabaseAccessor.insertOperations(mapOf(operation.getClientId() to listOf(operation)))
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        LOGGER.debug("Cash operation for client ${message.clientId}, asset ${message.assetId}, amount: ${message.amount} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }

    fun getAssetPair(assetPairId: String): AssetPair? {
        var assetPair = assetPairs[assetPairId]
        if (assetPair == null) {
            assetPair = walletDatabaseAccessor.loadAssetPair(assetPairId)
            if (assetPair != null) {
                assetPairs[assetPairId] = assetPair
            }
        }

        return assetPair
    }

    fun getBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return balance
            }
        }

        return 0.0
    }

    fun processWalletOperations(operations: List<WalletOperation>) {
        val walletsToAdd = LinkedList<Wallet>()
        operations.forEach { operation ->
            val client = balances.getOrPut(operation.getClientId()) { HashMap<String, Double>() }
            val balance = client[operation.assetId] ?: 0.0
            client.put(operation.assetId, balance + operation.amount)

            val wallet = wallets.getOrPut(operation.getClientId()) { Wallet( operation.getClientId() ) }
            wallet.addBalance(operation.assetId, operation.amount)

            if (!walletsToAdd.contains(wallet)) {
                walletsToAdd.add(wallet)
            }
        }

        walletDatabaseAccessor.insertOrUpdateWallets(walletsToAdd)
    }
}