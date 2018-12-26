package com.lykke.matching.engine.database.reconciliation

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.services.BalancesService
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.*

@Component
@Order(1)
class AccountsMigrationService @Autowired constructor (private val config: Config,
                                                       private val redisWalletDatabaseAccessor: Optional<RedisWalletDatabaseAccessor>,
                                                       private val balancesService: BalancesService): ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        if (config.me.walletsMigration) {
            migrateAccounts()
        }
    }

    companion object {
        private val LOGGER = Logger.getLogger(AccountsMigrationService::class.java.name)
    }

    private val azureAccountsTableName = config.me.db.accountsTableName
            ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME
    private val azureDatabaseAccessor = AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, azureAccountsTableName)

    fun migrateAccounts() {
        if (!config.me.walletsMigration) {
            return
        }
        if (config.me.redis == null) {
            throw IllegalArgumentException("Redis config is not provided can not migrate accounts")
        }

        when (config.me.storage) {
            Storage.Azure -> fromRedisToDb()
            Storage.RedisWithoutOrders,
            Storage.Redis -> fromDbToRedis()
        }
    }

    fun fromDbToRedis() {
        if (redisWalletDatabaseAccessor.get().loadWallets().isNotEmpty()) {
            throw AccountsMigrationException("Wallets already exist in redis ${config.me.redis.host}.${config.me.redis.port}")
        }

        val startTime = Date().time
        LOGGER.info("Starting wallets migration from azure to redis; azure table: $azureAccountsTableName, redis: ${config.me.redis.host}.${config.me.redis.port}")
        val wallets = azureDatabaseAccessor.loadWallets()
        val loadTime = Date().time
        LOGGER.info("Loaded ${wallets.size} wallets from azure (ms: ${loadTime - startTime})")
        val balancesSaved = balancesService.insertOrUpdateWallets(wallets.values.toList(), null)
        if (!balancesSaved) {
            LOGGER.error("Can not save balances data during migration from azure db to redis")
            return
        }
        val saveTime = Date().time
        LOGGER.info("Saved ${wallets.size} wallets to redis (ms: ${saveTime - loadTime})")

        compare()
    }

    fun fromRedisToDb() {
        val startTime = Date().time
        LOGGER.info("Starting wallets migration from redis to azure; redis: ${config.me.redis.host}.${config.me.redis.port}, azure table: $azureAccountsTableName")
        val loadTime = Date().time
        val wallets = redisWalletDatabaseAccessor.get().loadWallets()
        if (wallets.isEmpty()) {
            throw AccountsMigrationException("There are no wallets in redis ${config.me.redis.host}.${config.me.redis.port}")
        }
        LOGGER.info("Loaded ${wallets.size} wallets from redis (ms: ${loadTime - startTime})")
        val balancesSaved = balancesService.insertOrUpdateWallets(wallets.values.toList(), null)
        if (!balancesSaved) {
            LOGGER.error("Can not save balances data during migration from redis to azure db")
            return
        }
        val saveTime = Date().time
        LOGGER.info("Saved ${wallets.size} wallets to azure (ms: ${saveTime - loadTime})")

        compare()
    }

    /** Compares balances stored in redis & azure; logs comparison result  */
    private fun compare() {
        val azureWallets = azureDatabaseAccessor.loadWallets().filter { it.value.balances.isNotEmpty() }
        val redisWallets = redisWalletDatabaseAccessor.get().loadWallets()

        val onlyAzureClients = azureWallets.keys.filterNot { redisWallets.contains(it) }
        val onlyRedisClients = redisWallets.keys.filterNot { azureWallets.contains(it) }
        val commonClients = azureWallets.keys.filter { redisWallets.contains(it) }

        val differentWallets = LinkedList<String>()

        LOGGER.info("Comparison result. Differences: ")
        LOGGER.info("---------------------------------------------------------------------------------------------")
        commonClients.forEach {
            val azureWallet = azureWallets[it]
            val redisWallet = redisWallets[it]
            if (!compareBalances(azureWallet!!, redisWallet!!)) {
                differentWallets.add(it)
            }
        }
        LOGGER.info("---------------------------------------------------------------------------------------------")

        LOGGER.info("Total: ")
        LOGGER.info("azure clients count: ${azureWallets.size}")
        LOGGER.info("redis clients count: ${redisWallets.size}")
        LOGGER.info("only azure clients (count: ${onlyAzureClients.size}): $onlyAzureClients")
        LOGGER.info("only redis clients (count: ${onlyRedisClients.size}): $onlyRedisClients")
        LOGGER.info("clients with different wallets (count: ${differentWallets.size}): $differentWallets")
    }

    private fun compareBalances(azureWallet: Wallet, redisWallet: Wallet): Boolean {
        if (azureWallet.clientId != redisWallet.clientId) {
            LOGGER.info("different clients: ${azureWallet.clientId} & ${redisWallet.clientId}")
            return false
        }
        val clientId = azureWallet.clientId
        val azureBalances = azureWallet.balances
        val redisBalances = redisWallet.balances

        val onlyAzureAssets = azureBalances.keys.filterNot { redisBalances.keys.contains(it) || azureBalances[it]!!.balance.signum() == 0 }
        val onlyRedisAssets = redisBalances.keys.filterNot { azureBalances.keys.contains(it) }

        if (onlyAzureAssets.isNotEmpty() || onlyRedisAssets.isNotEmpty()) {
            LOGGER.info("different asset sets: $onlyAzureAssets & $onlyRedisAssets, client: $clientId")
            return false
        }

        val commonAssets = redisBalances.keys.filter { azureBalances.keys.contains(it) }
        commonAssets.forEach {
            val azureBalance = azureBalances[it]
            val redisBalance = redisBalances[it]
            if (azureBalance!!.balance != redisBalance!!.balance) {
                LOGGER.info("different balances: ${azureBalance.balance} & ${redisBalance.balance}, client: $clientId")
                return false
            }
            if (azureBalance.reserved != redisBalance.reserved) {
                LOGGER.info("different reserved balances: ${azureBalance.reserved} & ${redisBalance.reserved}, client: $clientId")
                return false
            }
        }

        return true
    }
}

class AccountsMigrationException(message: String) : MatchingEngineException(message)
