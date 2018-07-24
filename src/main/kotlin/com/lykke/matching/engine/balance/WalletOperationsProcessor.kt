package com.lykke.matching.engine.balance

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate as OutgoingBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

class WalletOperationsProcessor(private val balancesHolder: BalancesHolder,
                                private val applicationSettings: ApplicationSettingsCache,
                                private val persistenceManager: PersistenceManager,
                                private val balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                                private val assetsHolder: AssetsHolder,
                                private val validate: Boolean,
                                private val logger: Logger?) {

    companion object {
        private val LOGGER = Logger.getLogger(WalletOperationsProcessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val balancesUpdater = balancesHolder.createUpdater()
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
            changedAssetBalance.balance = NumberUtils.setScaleRoundHalfUp(changedAssetBalance.balance + operation.amount, asset.accuracy)
            changedAssetBalance.reserved = if (!applicationSettings.isTrustedClient(operation.clientId))
                NumberUtils.setScaleRoundHalfUp(changedAssetBalance.reserved + operation.reservedAmount, asset.accuracy)
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

    fun apply(): WalletOperationsProcessor {
        balancesUpdater.apply()
        return this
    }

    fun persistenceData(): BalancesData {
        return balancesUpdater.persistenceData()
    }

    fun persistBalances(processedMessage: ProcessedMessage?, messageSequenceNumber: Long?): Boolean {
        changedAssetBalances.forEach { it.value.apply() }
        return persistenceManager.persist(PersistenceData(persistenceData(),
                processedMessage,
                messageSequenceNumber))
    }

    fun sendNotification(id: String, type: String, messageId: String) {
        clientIds.forEach {  balanceUpdateNotificationQueue.put(BalanceUpdateNotification(it)) }
        if (updates.isNotEmpty()) {
            balancesHolder.sendBalanceUpdate(BalanceUpdate(id, type, Date(), updates.values.toList(), messageId))
        }
    }

    fun getClientBalanceUpdates(): List<ClientBalanceUpdate> {
        return updates.values.toList()
    }

    private fun defaultChangedAssetBalance(operation: WalletOperation): ChangedAssetBalance {
        val walletAssetBalance = balancesUpdater.getWalletAssetBalance(operation.clientId, operation.assetId)
        return ChangedAssetBalance(walletAssetBalance.wallet, walletAssetBalance.assetBalance)
    }
}

private abstract class AbstractChangedAssetBalance(val assetId: String,
                                                   val clientId: String,
                                                   val originBalance: BigDecimal,
                                                   val originReserved: BigDecimal) {
    var balance = originBalance
    var reserved = originReserved
}

private class ChangedAssetBalance(private val wallet: Wallet,
                                  val assetBalance: AssetBalance) :
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
        validateBalanceChange(assetBalance.clientId,
                assetBalance.assetId,
                assetBalance.originBalance,
                assetBalance.originReserved,
                assetBalance.balance,
                assetBalance.reserved)

@Throws(BalanceException::class)
fun validateBalanceChange(clientId: String, assetId: String, oldBalance: BigDecimal, oldReserved: BigDecimal, newBalance: BigDecimal, newReserved: BigDecimal) {
    val balanceInfo = "Invalid balance (client=$clientId, asset=$assetId, oldBalance=$oldBalance, oldReserved=$oldReserved, newBalance=$newBalance, newReserved=$newReserved)"

    // Balance can become negative earlier due to transfer operation with overdraftLimit > 0.
    // In this case need to check only difference of reserved & main balance.
    // It shouldn't be greater than previous one.
    if (newBalance < BigDecimal.ZERO && !(oldBalance < BigDecimal.ZERO && (oldBalance >= newBalance || oldReserved + newBalance >= newReserved + oldBalance))) {
        throw BalanceException(balanceInfo)
    }
    if (newReserved < BigDecimal.ZERO && oldReserved > newReserved) {
        throw BalanceException(balanceInfo)
    }

    // equals newBalance < newReserved && oldReserved - oldBalance < newReserved - newBalance
    if (newBalance < newReserved && oldReserved + newBalance < newReserved + oldBalance) {
        throw BalanceException(balanceInfo)
    }
}