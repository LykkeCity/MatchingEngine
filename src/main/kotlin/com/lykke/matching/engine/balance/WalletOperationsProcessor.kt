package com.lykke.matching.engine.balance

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolder
import com.lykke.matching.engine.order.transaction.WalletAssetBalance
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate as OutgoingBalanceUpdate

class WalletOperationsProcessor(private val balancesHolder: BalancesHolder,
                                private val currentTransactionBalancesHolder: CurrentTransactionBalancesHolder,
                                private val applicationSettingsHolder: ApplicationSettingsHolder,
                                private val persistenceManager: PersistenceManager,
                                private val assetsHolder: AssetsHolder,
                                private val logger: Logger?): BalancesGetter {

    companion object {
        private val LOGGER = Logger.getLogger(WalletOperationsProcessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

   private val clientBalanceUpdatesByClientIdAndAssetId = HashMap<String, ClientBalanceUpdate>()

    fun preProcess(operations: Collection<WalletOperation>, forceApply: Boolean = false): WalletOperationsProcessor {
        if (operations.isEmpty()) {
            return this
        }
        val changedAssetBalances = HashMap<String, ChangedAssetBalance>()
        operations.forEach { operation ->
            if (isTrustedClientReservedBalanceOperation(operation)) {
                return@forEach
            }
            val changedAssetBalance = changedAssetBalances.getOrPut(generateKey(operation)) {
                getChangedAssetBalance(operation.clientId, operation.assetId)
            }

            val asset = assetsHolder.getAsset(operation.assetId)
            changedAssetBalance.balance = NumberUtils.setScaleRoundHalfUp(changedAssetBalance.balance + operation.amount, asset.accuracy)
            changedAssetBalance.reserved = if (!applicationSettingsHolder.isTrustedClient(operation.clientId))
                NumberUtils.setScaleRoundHalfUp(changedAssetBalance.reserved + operation.reservedAmount, asset.accuracy)
            else
                changedAssetBalance.reserved
        }

        try {
            changedAssetBalances.values.forEach { validateBalanceChange(it) }
        } catch (e: BalanceException) {
            if (!forceApply) {
                throw e
            }
            val message = "Force applying of invalid balance: ${e.message}"
            (logger ?: LOGGER).error(message)
            METRICS_LOGGER.logError(message, e)
        }

        changedAssetBalances.forEach { processChangedAssetBalance(it.value) }
        return this
    }

    private fun processChangedAssetBalance(changedAssetBalance: ChangedAssetBalance) {
        if (!changedAssetBalance.isChanged()) {
            return
        }
        changedAssetBalance.apply()
        generateEventData(changedAssetBalance)
    }

    private fun generateEventData(changedAssetBalance: ChangedAssetBalance) {
        val key = generateKey(changedAssetBalance)
        val update = clientBalanceUpdatesByClientIdAndAssetId.getOrPut(key) {
            ClientBalanceUpdate(changedAssetBalance.clientId,
                    changedAssetBalance.assetId,
                    changedAssetBalance.originBalance,
                    changedAssetBalance.balance,
                    changedAssetBalance.originReserved,
                    changedAssetBalance.reserved)
        }
        update.newBalance = changedAssetBalance.balance
        update.newReserved = changedAssetBalance.reserved
        if (isBalanceUpdateNotificationNotNeeded(update)) {
            clientBalanceUpdatesByClientIdAndAssetId.remove(key)
        }
    }

    private fun isBalanceUpdateNotificationNotNeeded(clientBalanceUpdate: ClientBalanceUpdate): Boolean {
        return NumberUtils.equalsIgnoreScale(clientBalanceUpdate.oldBalance, clientBalanceUpdate.newBalance) &&
                NumberUtils.equalsIgnoreScale(clientBalanceUpdate.oldReserved, clientBalanceUpdate.newReserved)
    }

    fun apply(): WalletOperationsProcessor {
        currentTransactionBalancesHolder.apply()
        return this
    }

    fun persistenceData(): BalancesData {
        return currentTransactionBalancesHolder.persistenceData()
    }

    fun persistBalances(processedMessage: ProcessedMessage?,
                        orderBooksData: OrderBooksPersistenceData?,
                        stopOrderBooksData: OrderBooksPersistenceData?,
                        messageSequenceNumber: Long?): Boolean {
        return persistenceManager.persist(PersistenceData(persistenceData(),
                processedMessage,
                orderBooksData,
                stopOrderBooksData,
                messageSequenceNumber,
                null))
    }

    fun sendNotification(id: String, type: String, messageId: String) {
        if (clientBalanceUpdatesByClientIdAndAssetId.isNotEmpty()) {
            balancesHolder.sendBalanceUpdate(BalanceUpdate(id, type, Date(), clientBalanceUpdatesByClientIdAndAssetId.values.toList(), messageId))
        }
    }

    fun getClientBalanceUpdates(): List<ClientBalanceUpdate> {
        return clientBalanceUpdatesByClientIdAndAssetId.values.toList()
    }

    override fun getAvailableBalance(clientId: String, assetId: String): BigDecimal {
        val balance = getChangedCopyOrOriginalAssetBalance(clientId, assetId)
        return if (balance.reserved > BigDecimal.ZERO)
            balance.balance - balance.reserved
        else
            balance.balance
    }

    override fun getAvailableReservedBalance(clientId: String, assetId: String): BigDecimal {
        val balance = getChangedCopyOrOriginalAssetBalance(clientId, assetId)
        return if (balance.reserved > BigDecimal.ZERO && balance.reserved < balance.balance)
            balance.reserved
        else
            balance.balance
    }

    override fun getReservedBalance(clientId: String, assetId: String): BigDecimal {
        return getChangedCopyOrOriginalAssetBalance(clientId, assetId).reserved
    }

    private fun isTrustedClientReservedBalanceOperation(operation: WalletOperation): Boolean {
        return NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, operation.amount) && applicationSettingsHolder.isTrustedClient(operation.clientId)
    }

    private fun getChangedAssetBalance(clientId: String, assetId: String): ChangedAssetBalance {
        val walletAssetBalance = getCurrentTransactionWalletAssetBalance(clientId, assetId)
        return ChangedAssetBalance(walletAssetBalance.wallet, walletAssetBalance.assetBalance)
    }

    private fun getChangedCopyOrOriginalAssetBalance(clientId: String, assetId: String): AssetBalance {
        return currentTransactionBalancesHolder.getChangedCopyOrOriginalAssetBalance(clientId, assetId)
    }

    private fun getCurrentTransactionWalletAssetBalance(clientId: String, assetId: String): WalletAssetBalance {
        return currentTransactionBalancesHolder.getWalletAssetBalance(clientId, assetId)
    }
}

private class ChangedAssetBalance(private val wallet: Wallet,
                                  assetBalance: AssetBalance) {

    val assetId = assetBalance.asset
    val clientId = wallet.clientId
    val originBalance = assetBalance.balance
    val originReserved = assetBalance.reserved
    var balance = originBalance
    var reserved = originReserved

    fun isChanged(): Boolean {
        return !NumberUtils.equalsIgnoreScale(originBalance, balance) ||
                !NumberUtils.equalsIgnoreScale(originReserved, reserved)
    }

    fun apply(): Wallet {
        wallet.setBalance(assetId, balance)
        wallet.setReservedBalance(assetId, reserved)
        return wallet
    }
}

private fun generateKey(operation: WalletOperation) = generateKey(operation.clientId, operation.assetId)

private fun generateKey(assetBalance: ChangedAssetBalance) = generateKey(assetBalance.clientId, assetBalance.assetId)

private fun generateKey(clientId: String, assetId: String) = "${clientId}_$assetId"

@Throws(BalanceException::class)
private fun validateBalanceChange(assetBalance: ChangedAssetBalance) =
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