package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.daos.balance.ClientOrdersReservedVolume
import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.ReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureReservedVolumesDatabaseAccessor
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
import org.springframework.context.ApplicationContext
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.LinkedBlockingQueue

fun correctReservedVolumesIfNeed(config: Config, applicationContext: ApplicationContext) {
    if (!config.me.correctReservedVolumes) {
        return
    }
    val walletDatabaseAccessor = applicationContext.getBean(AzureWalletDatabaseAccessor::class.java)
    val dictionariesDatabaseAccessor = AzureDictionariesDatabaseAccessor(config.me.db.dictsConnString)
    val backOfficeDatabaseAccessor =  applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)
    val filePath = config.me.orderBookPath
    ReservedVolumesRecalculator.teeLog("Starting order books analyze, path: $filePath")
    val orderBookDatabaseAccessor = applicationContext.getBean(FileOrderBookDatabaseAccessor::class.java)
    val reservedVolumesDatabaseAccessor = applicationContext.getBean(AzureReservedVolumesDatabaseAccessor::class.java)
    ReservedVolumesRecalculator(walletDatabaseAccessor,
            dictionariesDatabaseAccessor,
            backOfficeDatabaseAccessor,
            orderBookDatabaseAccessor,
            reservedVolumesDatabaseAccessor,
            config.me.trustedClients,
            applicationContext).recalculate()
}


class ReservedVolumesRecalculator(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                  private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                                  private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor,
                                  private val reservedVolumesDatabaseAccessor: ReservedVolumesDatabaseAccessor,
                                  private val trustedClients: Set<String>,
                                  private val applicationContext: ApplicationContext) {


    companion object {
        private val LOGGER = Logger.getLogger(ReservedVolumesRecalculator::class.java.name)

        fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    fun recalculate() {
        val assetsHolder = AssetsHolder(AssetsCache(backOfficeDatabaseAccessor))
        val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(dictionariesDatabaseAccessor))
        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)

        val orders = orderBookDatabaseAccessor.loadLimitOrders()
        val reservedBalances = HashMap<String, MutableMap<String, ClientOrdersReservedVolume>>()
        var count = 1
        orders.forEach { order ->
            if (!trustedClients.contains(order.clientId)) {
                LOGGER.info("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, price:${order.price}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                val asset = assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)

                val reservedVolume = if (order.reservedLimitVolume != null) {
                    order.reservedLimitVolume!!
                } else {
                    val calculatedReservedVolume = if (order.isBuySide()) RoundingUtils.round(order.getAbsRemainingVolume() * order.price , asset.accuracy, false) else order.getAbsRemainingVolume()
                    LOGGER.info("Null reserved volume, recalculated: $calculatedReservedVolume")
                    calculatedReservedVolume
                }

                val clientAssets = reservedBalances.getOrPut(order.clientId) { HashMap() }
                val balance = clientAssets.getOrPut(asset.assetId) { ClientOrdersReservedVolume() }
                val newBalance = RoundingUtils.parseDouble(balance.volume + reservedVolume, asset.accuracy).toDouble()
                balance.volume = newBalance
                balance.orderIds.add(order.externalId)
            }
        }
        LOGGER.info("---------------------------------------------------------------------------------------------------")

        reservedBalances.forEach { client ->
            LOGGER.info("${client.key} : ${client.value}")
        }

        LOGGER.info("---------------------------------------------------------------------------------------------------")

        val corrections = LinkedList<ReservedVolumeCorrection>()
        balanceHolder.wallets.forEach {
            val wallet = it.value
            val id = wallet.clientId
            wallet.balances.values.forEach {
                val oldBalance = it.reserved
                val newBalance = reservedBalances[id]?.get(it.asset)
                if (newBalance != null && newBalance.volume > 0.0) {
                    if (oldBalance != newBalance.volume) {
                        val correction = ReservedVolumeCorrection(id, it.asset, newBalance.orderIds.joinToString(","), oldBalance, newBalance.volume)
                        corrections.add(correction)
                        teeLog("1 $id, ${it.asset} : Old $oldBalance New $newBalance")
                        wallet.setReservedBalance(it.asset, newBalance.volume)
                        walletDatabaseAccessor.insertOrUpdateWallet(wallet)
                    }
                } else if (oldBalance != 0.0) {
                    val orderIds = if (newBalance != null) newBalance.orderIds.joinToString(",") else null
                    val correction = ReservedVolumeCorrection(id, it.asset, orderIds, oldBalance, newBalance?.volume ?: 0.0)
                    corrections.add(correction)
                    teeLog("2 $id, ${it.asset} : Old $oldBalance New ${newBalance ?: 0.0}")
                    wallet.setReservedBalance(it.asset, 0.0)
                    walletDatabaseAccessor.insertOrUpdateWallet(wallet)
                }
            }
        }
        reservedVolumesDatabaseAccessor.addCorrectionsInfo(corrections)
    }

}