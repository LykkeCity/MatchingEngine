package com.lykke.matching.engine.balance

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class WalletOperationsProcessor(private val balancesHolder: BalancesHolder,
                                private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                private val notificationQueue: BlockingQueue<BalanceUpdateNotification>,
                                private val assetsHolder: AssetsHolder,
                                private val validate: Boolean,
                                private val logger: Logger?) {

    companion object {
        private val LOGGER = Logger.getLogger(WalletOperationsProcessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val changedAssetBalances = HashMap<String, ChangedAssetBalance>()
    private val clientIds = HashSet<String>()
    private val updates = HashMap<String, ClientBalanceUpdate>()

    fun preProcess(operations: List<WalletOperation>, forceApply: Boolean = false): WalletOperationsProcessor {
        if (operations.isEmpty()) {
            return this
        }
        val transactionChangedAssetBalances = HashMap<String, TransactionChangedAssetBalance>()
        operations.forEach { operation ->
            val key = key(operation)
            val changedAssetBalance = transactionChangedAssetBalances.getOrPut(key) {
                TransactionChangedAssetBalance(changedAssetBalances.getOrDefault(key, defaultChangedAssetBalance(operation)))
            }

            val asset = assetsHolder.getAsset(operation.assetId)
            changedAssetBalance.balance = NumberUtils.parseDouble(changedAssetBalance.balance + operation.amount, asset.accuracy).toDouble()
            changedAssetBalance.reserved = if (!balancesHolder.isTrustedClient(operation.clientId))
                NumberUtils.parseDouble(changedAssetBalance.reserved + operation.reservedAmount, asset.accuracy).toDouble()
            else
                changedAssetBalance.reserved
        }

        if (validate) {
            try {
                transactionChangedAssetBalances.values.forEach { validateBalanceChange(it) }
            } catch (e: BalanceException) {
                if (!forceApply) {
                    throw e
                }
                val message = "Force applying of invalid balance: ${e.message}"
                (logger ?: LOGGER).error(message)
                METRICS_LOGGER.logError(message, e)
            }
        }

        changedAssetBalances.putAll(transactionChangedAssetBalances.mapValues {
            val transactionChangedAssetBalance = it.value
            clientIds.add(transactionChangedAssetBalance.clientId)
            val update = updates.getOrPut(key(transactionChangedAssetBalance)) {
                ClientBalanceUpdate(transactionChangedAssetBalance.clientId,
                        transactionChangedAssetBalance.assetId,
                        transactionChangedAssetBalance.changedAssetBalance.originBalance,
                        transactionChangedAssetBalance.balance,
                        transactionChangedAssetBalance.changedAssetBalance.originReserved,
                        transactionChangedAssetBalance.reserved)
            }
            update.newBalance = transactionChangedAssetBalance.balance
            update.newReserved = transactionChangedAssetBalance.reserved
            it.value.apply()
        })
        return this
    }

    fun apply(id: String, type: String) {
        if (changedAssetBalances.isEmpty()) {
            return
        }
        val updatedWallets = changedAssetBalances.values.mapTo(HashSet()) { it.apply() }
        walletDatabaseAccessor.insertOrUpdateWallets(updatedWallets.toList())
        clientIds.forEach { notificationQueue.put(BalanceUpdateNotification(it)) }
        balancesHolder.sendBalanceUpdate(BalanceUpdate(id, type, Date(), updates.values.toList()))
    }

    private fun defaultChangedAssetBalance(operation: WalletOperation): ChangedAssetBalance {
        val wallet = balancesHolder.wallets.getOrPut(operation.clientId) {
            Wallet(operation.clientId)
        }
        val assetBalance = wallet.balances.getOrPut(operation.assetId) {
            AssetBalance(operation.assetId)
        }
        return ChangedAssetBalance(wallet, assetBalance)
    }
}

private abstract class AbstractChangedAssetBalance(val assetId: String,
                                                   val clientId: String,
                                                   val originBalance: Double,
                                                   val originReserved: Double) {
    var balance: Double = originBalance
    var reserved: Double = originReserved
}

private class ChangedAssetBalance(private val wallet: Wallet,
                                  assetBalance: AssetBalance) :
        AbstractChangedAssetBalance(assetBalance.asset,
                wallet.clientId,
                assetBalance.balance,
                assetBalance.reserved) {

    fun apply(): Wallet {
        wallet.setBalance(assetId, balance)
        wallet.setReservedBalance(assetId, reserved)
        return wallet
    }
}

private class TransactionChangedAssetBalance(val changedAssetBalance: ChangedAssetBalance) :
        AbstractChangedAssetBalance(changedAssetBalance.assetId,
                changedAssetBalance.clientId,
                changedAssetBalance.balance,
                changedAssetBalance.reserved) {

    fun apply(): ChangedAssetBalance {
        changedAssetBalance.balance = balance
        changedAssetBalance.reserved = reserved
        return changedAssetBalance
    }
}

private fun key(operation: WalletOperation) = "${operation.clientId}_${operation.assetId}"

private fun key(assetBalance: TransactionChangedAssetBalance) = "${assetBalance.clientId}_${assetBalance.assetId}"

@Throws(BalanceException::class)
private fun validateBalanceChange(assetBalance: TransactionChangedAssetBalance) =
        validateBalanceChange(assetBalance.clientId, assetBalance.assetId, assetBalance.originBalance, assetBalance.originReserved, assetBalance.balance, assetBalance.reserved)

@Throws(BalanceException::class)
fun validateBalanceChange(clientId: String, assetId: String, oldBalance: Double, oldReserved: Double, newBalance: Double, newReserved: Double) {
    val balanceInfo = "Invalid balance (client=$clientId, asset=$assetId, oldBalance=$oldBalance, oldReserved=$oldReserved, newBalance=$newBalance, newReserved=$newReserved)"

    // Balance can become negative earlier due to transfer operation with overdraftLimit > 0.
    // In this case need to check only difference of reserved & main balance.
    // It shouldn't be greater than previous one.
    if (newBalance < 0.0 && !(oldBalance < 0.0 && (oldBalance >= newBalance || oldReserved + newBalance >= newReserved + oldBalance))) {
        throw BalanceException(balanceInfo)
    }
    if (newReserved < 0.0 && oldReserved > newReserved) {
        throw BalanceException(balanceInfo)
    }

    // equals newBalance < newReserved && oldReserved - oldBalance < newReserved - newBalance
    if (newBalance < newReserved && oldReserved + newBalance < newReserved + oldBalance) {
        throw BalanceException(balanceInfo)
    }
}