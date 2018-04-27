package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import org.apache.log4j.Logger
import redis.clients.jedis.Jedis
import java.util.Date
import java.util.LinkedList

fun migrateAccountsIfConfigured(config: Config) {
    if (!config.me.walletsMigration) {
        return
    }
    when (config.me.walletsStorage) {
        WalletsStorage.Azure -> AccountsMigration(config.me).fromRedisToDb()
        WalletsStorage.Redis -> AccountsMigration(config.me).fromDbToRedis()
    }
}

class AccountsMigrationException(message: String) : MatchingEngineException(message)

class AccountsMigration(config: MatchingEngineConfig) {

    companion object {
        private val LOGGER = Logger.getLogger(AccountsMigration::class.java.name)
    }

    private val azureAccountsTableName = config.db.accountsTableName
            ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME

    private val redisHost = config.redis.balancesHost
    private val redisPort = config.redis.balancesPort
    private val redisIndex = config.redis.balancesDbIndex

    private val azureDatabaseAccessor = AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, azureAccountsTableName)
    private val redisDatabaseAccessor: RedisWalletDatabaseAccessor

    init {
        val jedis = Jedis(redisHost, redisPort)
        jedis.select(redisIndex)
        redisDatabaseAccessor = RedisWalletDatabaseAccessor(jedis)
    }


    fun fromDbToRedis() {
        if (redisDatabaseAccessor.loadWallets().isNotEmpty()) {
            throw AccountsMigrationException("Wallets already exist in redis $redisHost:$redisPort, index: $redisIndex")
        }

        val startTime = Date().time
        teeLog("Starting wallets migration from azure to redis; azure table: $azureAccountsTableName, redis: $redisHost:$redisPort, index: $redisIndex")
        val wallets = azureDatabaseAccessor.loadWallets()
        val loadTime = Date().time
        teeLog("Loaded ${wallets.size} wallets from azure (ms: ${loadTime - startTime})")
        redisDatabaseAccessor.insertOrUpdateWallets(wallets.values.toList())
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to redis (ms: ${saveTime - loadTime})")

        compare()
    }

    fun fromRedisToDb() {
        val startTime = Date().time
        teeLog("Starting wallets migration from redis to azure; redis: $redisHost:$redisPort, azure table: $azureAccountsTableName")
        val loadTime = Date().time
        val wallets = redisDatabaseAccessor.loadWallets()
        if (wallets.isEmpty()) {
            throw AccountsMigrationException("There are no wallets in redis $redisHost:$redisPort, index: $redisIndex")
        }
        teeLog("Loaded ${wallets.size} wallets from redis (ms: ${loadTime - startTime})")
        azureDatabaseAccessor.insertOrUpdateWallets(wallets.values.toList())
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to azure (ms: ${saveTime - loadTime})")

        compare()
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }

    /** Compares balances stored in redis & azure; logs comparison result  */
    private fun compare() {
        val azureWallets = azureDatabaseAccessor.loadWallets().filter { it.value.balances.isNotEmpty() }
        val redisWallets = redisDatabaseAccessor.loadWallets()

        val onlyAzureClients = azureWallets.keys.filterNot { redisWallets.contains(it) }
        val onlyRedisClients = redisWallets.keys.filterNot { azureWallets.contains(it) }
        val commonClients = azureWallets.keys.filter { redisWallets.contains(it) }

        val differentWallets = LinkedList<String>()

        teeLog("Comparison result. Differences: ")
        teeLog("---------------------------------------------------------------------------------------------")
        commonClients.forEach {
            val azureWallet = azureWallets[it]
            val redisWallet = redisWallets[it]
            if (!compareBalances(azureWallet!!, redisWallet!!)) {
                differentWallets.add(it)
            }
        }
        teeLog("---------------------------------------------------------------------------------------------")

        teeLog("Total: ")
        teeLog("azure clients count: ${azureWallets.size}")
        teeLog("redis clients count: ${redisWallets.size}")
        teeLog("only azure clients (count: ${onlyAzureClients.size}): $onlyAzureClients")
        teeLog("only redis clients (count: ${onlyRedisClients.size}): $onlyRedisClients")
        teeLog("clients with different wallets (count: ${differentWallets.size}): $differentWallets")
    }

    private fun compareBalances(azureWallet: Wallet, redisWallet: Wallet): Boolean {
        if (azureWallet.clientId != redisWallet.clientId) {
            teeLog("different clients: ${azureWallet.clientId} & ${redisWallet.clientId}")
            return false
        }
        val clientId = azureWallet.clientId
        val azureBalances = azureWallet.balances
        val redisBalances = redisWallet.balances

        val onlyAzureAssets = azureBalances.keys.filterNot { redisBalances.keys.contains(it) || azureBalances[it]!!.balance.signum() == 0 }
        val onlyRedisAssets = redisBalances.keys.filterNot { azureBalances.keys.contains(it) }

        if (onlyAzureAssets.isNotEmpty() || onlyRedisAssets.isNotEmpty()) {
            teeLog("different asset sets: $onlyAzureAssets & $onlyRedisAssets, client: $clientId")
            return false
        }

        val commonAssets = redisBalances.keys.filter { azureBalances.keys.contains(it) }
        commonAssets.forEach {
            val azureBalance = azureBalances[it]
            val redisBalance = redisBalances[it]
            if (azureBalance!!.balance != redisBalance!!.balance) {
                teeLog("different balances: ${azureBalance.balance} & ${redisBalance.balance}, client: $clientId")
                return false
            }
            if (azureBalance.reserved != redisBalance.reserved) {
                teeLog("different reserved balances: ${azureBalance.reserved} & ${redisBalance.reserved}, client: $clientId")
                return false
            }
        }

        return true
    }
}