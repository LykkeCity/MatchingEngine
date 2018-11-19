package com.lykke.matching.engine.holders

import com.lykke.matching.engine.balance.BalancesGetter
import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolder
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue

@Component
class BalancesHolder(private val balancesDbAccessorsHolder: BalancesDatabaseAccessorsHolder,
                     private val balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                     private val persistenceManager: PersistenceManager,
                     private val assetsHolder: AssetsHolder,
                     private val balanceUpdateQueue: BlockingQueue<BalanceUpdate>,
                     private val applicationSettingsCache: ApplicationSettingsCache): BalancesGetter {

    companion object {
        private val LOGGER = Logger.getLogger(BalancesHolder::class.java.name)
    }

    lateinit var wallets: MutableMap<String, Wallet>
    var initialClientsCount = 0
    var initialBalancesCount = 0

    init {
        update()
    }

    private fun update() {
        wallets = balancesDbAccessorsHolder.primaryAccessor.loadWallets()
        initialClientsCount = wallets.size
        initialBalancesCount = wallets.values.sumBy { it.balances.size }
    }

    fun clientExists(clientId: String): Boolean {
        return wallets.containsKey(clientId)
    }

    fun getBalances(clientId: String): Map<String, AssetBalance> {
        val wallet = wallets[clientId]
        return wallet?.balances ?: emptyMap()
    }

    fun getBalance(clientId: String, assetId: String): BigDecimal {
        return getBalances(clientId)[assetId]?.balance ?: BigDecimal.ZERO
    }

    override fun getReservedBalance(clientId: String, assetId: String): BigDecimal {
        return getBalances(clientId)[assetId]?.reserved ?: BigDecimal.ZERO
    }

    override fun getAvailableBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return (if (balance.reserved > BigDecimal.ZERO)
                    balance.balance - balance.reserved
                else balance.balance)
            }
        }

        return BigDecimal.ZERO
    }

    override fun getAvailableReservedBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                // reserved can be greater than base balance due to transfer with overdraft
                return if (balance.reserved.signum() == 1 && balance.reserved <= balance.balance) balance.reserved else balance.balance
            }
        }

        return BigDecimal.ZERO
    }

    fun updateBalance(processedMessage: ProcessedMessage?,
                      messageSequenceNumber: Long?,
                      clientId: String,
                      assetId: String,
                      balance: BigDecimal): Boolean {
        val currentTransactionBalancesHolder = createCurrentTransactionBalancesHolder()
        currentTransactionBalancesHolder.updateBalance(clientId, assetId, balance)
        val balancesData = currentTransactionBalancesHolder.persistenceData()
        val persisted = persistenceManager.persist(PersistenceData(balancesData, processedMessage, null, null, messageSequenceNumber, null))
        if (!persisted) {
            return false
        }
        currentTransactionBalancesHolder.apply()
        balanceUpdateNotificationQueue.put(BalanceUpdateNotification(clientId))
        return true
    }
    fun updateReservedBalance(processedMessage: ProcessedMessage?,
                              messageSequenceNumber: Long?,
                              clientId: String,
                              assetId: String,
                              balance: BigDecimal,
                              skipForTrustedClient: Boolean = true): Boolean {
        val currentTransactionBalancesHolder = createCurrentTransactionBalancesHolder()
        currentTransactionBalancesHolder.updateReservedBalance(clientId, assetId, balance)
        val balancesData = currentTransactionBalancesHolder.persistenceData()
        val persisted = persistenceManager.persist(PersistenceData(balancesData, processedMessage, null, null, messageSequenceNumber, null))
        if (!persisted) {
            return false
        }
        currentTransactionBalancesHolder.apply()
        balanceUpdateNotificationQueue.put(BalanceUpdateNotification(clientId))
        return true
    }
    fun insertOrUpdateWallets(wallets: Collection<Wallet>, messageSequenceNumber: Long?) {
        persistenceManager.persist(PersistenceData(BalancesData(wallets, wallets.flatMap { it.balances.values }),
                null,
                null,
                null,
                midPricePersistenceData = null,
                messageSequenceNumber = messageSequenceNumber))
        update()
    }
    fun sendBalanceUpdate(balanceUpdate: BalanceUpdate) {
        if (balanceUpdate.balances.isNotEmpty()) {
            LOGGER.info(balanceUpdate.toString())
            balanceUpdateQueue.put(balanceUpdate)
        }
    }
    fun isTrustedClient(clientId: String) = applicationSettingsCache.isTrustedClient(clientId)
    fun createWalletProcessor(logger: Logger?, validate: Boolean = true): WalletOperationsProcessor {
        return WalletOperationsProcessor(this,
                createCurrentTransactionBalancesHolder(),
                applicationSettingsCache,
                persistenceManager,
                balanceUpdateNotificationQueue,
                assetsHolder,
                validate,
                logger)
    }
    private fun createCurrentTransactionBalancesHolder() = CurrentTransactionBalancesHolder(this)
    fun setWallets(wallets: Collection<Wallet>) {
        wallets.forEach { wallet ->
            this.wallets[wallet.clientId] = wallet
        }
    }
}