package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import java.util.HashMap
import java.util.concurrent.LinkedBlockingQueue

class ReservedVolumesRecalculator {
    companion object {
        val LOGGER = Logger.getLogger(ReservedVolumesRecalculator::class.java.name)
    }

    fun recalculate(config: Config) {
        val walletDatabaseAccessor = AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.dictsConnString)
        val assetsHolder = AssetsHolder(AssetsCache(AzureBackOfficeDatabaseAccessor(config.me.db.multisigConnString, config.me.db.bitCoinQueueConnectionString, config.me.db.dictsConnString), 60000))
        val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.dictsConnString), 60000))
        val balanceHolder = BalancesHolder(walletDatabaseAccessor, assetsHolder, LinkedBlockingQueue(), LinkedBlockingQueue(), config.me.trustedClients)

        val filePath = config.me.orderBookPath
        teeLog("Starting order books analyze, path: $filePath")
        val fileOrderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(filePath)
        val orders = fileOrderBookDatabaseAccessor.loadLimitOrders()
        val reservedBalances = HashMap<String, MutableMap<String, Double>>()
        var count = 1
        orders.forEach { order ->
            if (!config.me.trustedClients.contains(order.clientId)) {
                teeLog("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, price:${order.price}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                if (order.reservedLimitVolume != null) {
                    val clientAssets = reservedBalances.getOrPut(order.clientId) { HashMap()}
                    val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                    val asset = assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
                    val balance = clientAssets.getOrDefault(asset.assetId, 0.0)
                    val newBalance = RoundingUtils.parseDouble(balance + (order.reservedLimitVolume ?: 0.0), asset.accuracy).toDouble()
                    clientAssets[asset.assetId] = newBalance
                } else {
                    teeLog("Reserved volume is null")
                }
            }
        }
        teeLog("---------------------------------------------------------------------------------------------------")

        reservedBalances.forEach { client ->
            teeLog("${client.key} : ${client.value}")
        }

        teeLog("---------------------------------------------------------------------------------------------------")

        balanceHolder.wallets.forEach {
            val wallet = it.value
            val id = wallet.clientId
            wallet.balances.values.forEach {
                val oldBalance = it.reserved
                val newBalance = reservedBalances[id]?.get(it.asset)
                if (newBalance != null && newBalance > 0.0) {
                    if (oldBalance != newBalance) {
                        teeLog("1 $id, ${it.asset} : Old $oldBalance New $newBalance")
                        wallet.setReservedBalance(it.asset, newBalance)
                        walletDatabaseAccessor.insertOrUpdateWallet(wallet)
                    }
                } else if (oldBalance > 0) {
                    teeLog("2 $id, ${it.asset} : Old $oldBalance New ${newBalance?:0.0}")
                    wallet.setReservedBalance(it.asset, 0.0)
                    walletDatabaseAccessor.insertOrUpdateWallet(wallet)
                }
            }
        }
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}