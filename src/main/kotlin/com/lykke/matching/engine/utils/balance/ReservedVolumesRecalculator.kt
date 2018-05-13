package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.balance.ClientOrdersReservedVolume
import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.ReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.context.ApplicationContext
import java.math.BigDecimal
import java.util.HashMap
import java.util.LinkedList

fun correctReservedVolumesIfNeed(config: Config, applicationContext: ApplicationContext) {
    if (!config.me.correctReservedVolumes) {
        return
    }
    val backOfficeDatabaseAccessor =  applicationContext.getBean(BackOfficeDatabaseAccessor::class.java)
    val filePath = config.me.orderBookPath
    val stopOrderBookPath = config.me.stopOrderBookPath
    val stopOrderBookDatabaseAccessor = FileStopOrderBookDatabaseAccessor(stopOrderBookPath)
    ReservedVolumesRecalculator.teeLog("Starting order books analyze, path: $filePath")
    val orderBookDatabaseAccessor = applicationContext.getBean(OrderBookDatabaseAccessor::class.java)
    val reservedVolumesDatabaseAccessor = applicationContext.getBean(ReservedVolumesDatabaseAccessor::class.java)
    ReservedVolumesRecalculator(backOfficeDatabaseAccessor,
            orderBookDatabaseAccessor,
            stopOrderBookDatabaseAccessor,
            reservedVolumesDatabaseAccessor,
            applicationContext).recalculate()
}


class ReservedVolumesRecalculator(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                                  private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor,
                                  private val stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor,
                                  private val reservedVolumesDatabaseAccessor: ReservedVolumesDatabaseAccessor,
                                  private val applicationContext: ApplicationContext) {


    companion object {
        private val LOGGER = Logger.getLogger(ReservedVolumesRecalculator::class.java.name)

        fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    fun recalculate() {
        val assetsHolder = applicationContext.getBean(AssetsHolder::class.java)
        val assetsPairsHolder = applicationContext.getBean(AssetsPairsHolder::class.java)
        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)

        val applicationSettingsCache = applicationContext.getBean(ApplicationSettingsCache::class.java)

        val orders = orderBookDatabaseAccessor.loadLimitOrders()
        val stopOrders = stopOrderBookDatabaseAccessor.loadStopLimitOrders()

        val reservedBalances = HashMap<String, MutableMap<String, ClientOrdersReservedVolume>>()
        var count = 1

        val handleOrder: (order: NewLimitOrder, isStopOrder: Boolean) -> Unit = {order, isStopOrder->
            if (!applicationSettingsCache.isTrustedClient(order.clientId)) {
                try {
                    if (isStopOrder) {
                        LOGGER.info("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, lowerLimitPrice:${order.lowerLimitPrice}, lowerPrice:${order.lowerPrice}, upperLimitPrice:${order.upperLimitPrice}, upperPrice:${order.upperPrice}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                    } else {
                        LOGGER.info("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, price:${order.price}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                    }
                    val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                    val asset = assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)

                    val reservedVolume = if (order.reservedLimitVolume != null) {
                        order.reservedLimitVolume!!
                    } else {
                        val calculatedReservedVolume = if (order.isBuySide())
                            NumberUtils.round(if (isStopOrder) order.volume * (order.upperPrice ?: order.lowerPrice)!!
                            else order.getAbsRemainingVolume() * order.price, asset.accuracy, false)
                        else order.getAbsRemainingVolume()
                        LOGGER.info("Null reserved volume, recalculated: $calculatedReservedVolume")
                        calculatedReservedVolume
                    }

                    val clientAssets = reservedBalances.getOrPut(order.clientId) { HashMap() }
                    val balance = clientAssets.getOrPut(asset.assetId) { ClientOrdersReservedVolume() }
                    val newBalance = NumberUtils.parseDouble(balance.volume + reservedVolume, asset.accuracy).toDouble()
                    balance.volume = newBalance
                    balance.orderIds.add(order.externalId)
                } catch (e: Exception) {
                    val errorMessage = "Unable to handle order (id: ${order.externalId}): ${e.message}"
                    teeLog(errorMessage)
                }
            }
        }

        orders.forEach { handleOrder(it, false) }
        stopOrders.forEach { handleOrder(it, true) }

        LOGGER.info("---------------------------------------------------------------------------------------------------")

        reservedBalances.forEach { client ->
            LOGGER.info("${client.key} : ${client.value}")
        }

        LOGGER.info("---------------------------------------------------------------------------------------------------")

        val corrections = LinkedList<ReservedVolumeCorrection>()
        val updatedWallets = mutableSetOf<Wallet>()
        balanceHolder.wallets.forEach {
            val wallet = it.value
            val id = wallet.clientId
            wallet.balances.values.forEach {
                val oldBalance = it.reserved.toDouble()
                val newBalance = reservedBalances[id]?.get(it.asset)
                if (newBalance != null && newBalance.volume > 0.0) {
                    if (oldBalance != newBalance.volume) {
                        val correction = ReservedVolumeCorrection(id, it.asset, newBalance.orderIds.joinToString(","), oldBalance, newBalance.volume)
                        corrections.add(correction)
                        teeLog("1 $id, ${it.asset} : Old $oldBalance New $newBalance")
                        wallet.setReservedBalance(it.asset, newBalance.volume.toBigDecimal())
                        updatedWallets.add(wallet)
                    }
                } else if (oldBalance != 0.0) {
                    val orderIds = if (newBalance != null) newBalance.orderIds.joinToString(",") else null
                    val correction = ReservedVolumeCorrection(id, it.asset, orderIds, oldBalance, newBalance?.volume ?: 0.0)
                    corrections.add(correction)
                    teeLog("2 $id, ${it.asset} : Old $oldBalance New ${newBalance ?: 0.0}")
                    wallet.setReservedBalance(it.asset, BigDecimal.ZERO)
                    updatedWallets.add(wallet)
                }
            }
        }
        if (updatedWallets.isNotEmpty()) {
            balanceHolder.insertOrUpdateWallets(updatedWallets)
        }
        reservedVolumesDatabaseAccessor.addCorrectionsInfo(corrections)
    }

}