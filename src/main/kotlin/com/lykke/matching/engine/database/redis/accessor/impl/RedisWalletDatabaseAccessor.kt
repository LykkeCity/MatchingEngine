package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.utils.logging.MetricsLogger
import org.slf4j.LoggerFactory
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction
import java.util.*

class RedisWalletDatabaseAccessor(private val redisConnection: RedisConnection, private val balancesDatabase: Int) : WalletDatabaseAccessor {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(RedisWalletDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val KEY_PREFIX_BALANCE = "Balances:"
        private const val KEY_SEPARATOR = ":"
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        redisConnection.resource { jedis ->
            var balancesCount = 0

            jedis.select(balancesDatabase)
            val keys = balancesKeys(jedis).toList()

            val values = if (keys.isNotEmpty())
                jedis.mget(*keys.map { it.toByteArray() }.toTypedArray())
            else emptyList()

            values.forEachIndexed { index, value ->
                val key = keys[index]
                try {
                    if (value == null) {
                        throw Exception("Balance is not exist, key: $key")
                    }
                    val balance = deserializeClientAssetBalance(value)
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

            LOGGER.info("Loaded ${result.size} wallets, $balancesCount balances")
        }

        return result
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        // Nothing to do
    }

    override fun insertOrUpdateWallet(wallet: Wallet) {
        // Nothing to do
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

    private fun deserializeClientAssetBalance(value: ByteArray): AssetBalance {
        val readCase = conf.asObject(value)
        return readCase as AssetBalance
    }

    private fun key(balance: AssetBalance): String {
        return "$KEY_PREFIX_BALANCE${balance.clientId}$KEY_SEPARATOR${balance.asset}"
    }
}