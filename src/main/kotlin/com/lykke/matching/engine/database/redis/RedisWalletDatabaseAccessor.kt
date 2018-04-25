package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction
import java.util.HashMap

class RedisWalletDatabaseAccessor(private val jedisPool: JedisPoolHolder) : WalletDatabaseAccessor/*, BalancesDatabaseAccessor*/ {

    companion object {
        private val LOGGER = Logger.getLogger(RedisWalletDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val KEY_PREFIX_BALANCE = "Balance_"
        private const val KEY_SEPARATOR = "_"
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        var balancesCount = 0
        jedisPool.resource().use { jedis ->
            val keys = balancesKeys(jedis)
            keys.forEach { key ->
                try {
                    val balance = deserializeClientAssetBalance(jedis, key)
                    if (!key.removePrefix(KEY_PREFIX_BALANCE).startsWith(balance.clientId)) {
                        throw Exception("Invalid clientId: ${balance.clientId}, balance key: $key")
                    }
                    if (key.removePrefix("$KEY_PREFIX_BALANCE${balance.clientId}$KEY_SEPARATOR") != balance.asset) {
                        throw Exception("Invalid assetId: ${balance.asset}, balance key: $key")
                    }
                    val clientBalances = result.getOrPut(balance.clientId) { Wallet(balance.clientId) }
                    clientBalances.balances[balance.asset] = balance
                    balancesCount++
                } catch (e: Exception) {
                    val message = "Unable to load, balanceKey: $key"
                    LOGGER.error(message, e)
                    METRICS_LOGGER.logError(message, e)
                }
            }
        }
        LOGGER.info("Loaded ${result.size} wallets, $balancesCount balances")
        return result
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        jedisPool.resource().use { jedis ->
            val transaction = jedis.multi()
            var success = false
            try {
                insertOrUpdateBalances(transaction, wallets.flatMap { it.balances.values })
                success = true
            } finally {
                if (success) transaction.exec() else jedis.resetState()
            }
        }
    }

    override fun insertOrUpdateWallet(wallet: Wallet) {
        insertOrUpdateWallets(listOf(wallet))
    }

    fun insertOrUpdateBalances(transaction: Transaction, balances: Collection<AssetBalance>) {
        balances.forEach { clientAssetBalance ->
            transaction.set(key(clientAssetBalance).toByteArray(), serializeClientAssetBalance(clientAssetBalance))
        }
    }

    private fun balancesKeys(jedis: Jedis): Set<String> {
        return jedis.keys("$KEY_PREFIX_BALANCE*")
    }

    private fun serializeClientAssetBalance(balance: AssetBalance) = conf.asByteArray(balance)

    private fun deserializeClientAssetBalance(jedis: Jedis, key: String): AssetBalance {
        val bytes = jedis[key.toByteArray()]
        val readCase = conf.asObject(bytes)
        return readCase as AssetBalance
    }

    private fun key(balance: AssetBalance): String {
        return "$KEY_PREFIX_BALANCE${balance.clientId}$KEY_SEPARATOR${balance.asset}"
    }

}