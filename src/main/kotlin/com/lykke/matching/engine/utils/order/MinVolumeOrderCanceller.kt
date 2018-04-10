package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.cancel.LimitOrdersCancellerFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID

class MinVolumeOrderCanceller(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val genericLimitOrderService: GenericLimitOrderService,
                              private val cancellerFactory: LimitOrdersCancellerFactory) {

    companion object {
        private val LOGGER = Logger.getLogger(MinVolumeOrderCanceller::class.java.name)

        private fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)

    fun cancel() {
        val operationId = UUID.randomUUID().toString()
        teeLog("Starting order books analyze to cancel min volume orders ($operationId)")

        val ordersToCancel = HashMap<AssetPair, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
        val ordersToRemove = HashMap<String, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
        var totalCount = 0
        val checkAndAddToCancel: (order: NewLimitOrder) -> Unit = { order ->
            try {
                val assetPair = try {
                    assetsPairsHolder.getAssetPair(order.assetPairId)
                } catch (e: Exception) {
                    dictionariesDatabaseAccessor.loadAssetPair(order.assetPairId, true)
                }

                if (assetPair == null) {
                    // assetPair == null means asset pair is not found in dictionary => remove this order (without reserved funds recalculation)
                    teeLog("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} is not found")
                    ordersToRemove.getOrPut(order.assetPairId) { HashMap() }.getOrPut(order.isBuySide()) { LinkedList() }.add(order)
                    totalCount++
                } else if (assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume) {
                    teeLog("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} min volume is ${assetPair.minVolume}, remaining volume is ${order.getAbsRemainingVolume()}")
                    ordersToCancel.getOrPut(assetPair) { HashMap() }.getOrPut(order.isBuySide()) { LinkedList() }.add(order)
                    totalCount++
                }
            } catch (e: Exception) {
                teeLog("Unable to check order (${order.externalId}): ${e.message}. Skipped.")
            }
        }

        genericLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            orderBook.getOrderBook(true).forEach { order -> checkAndAddToCancel(order) }
            orderBook.getOrderBook(false).forEach { order -> checkAndAddToCancel(order) }
        }

        teeLog("Starting orders cancellation (orders count: $totalCount)")
        try {
            cancellerFactory.create(Date())
                    .preProcess(ordersToCancel, ordersToRemove)
                    .applyFull(operationId, MessageType.LIMIT_ORDER.name)
        } catch (e: BalanceException) {
            teeLog("Unable to process wallet operations due to invalid balance: ${e.message}")
            return
        }

        teeLog("Min volume orders cancellation is finished")
    }
}
