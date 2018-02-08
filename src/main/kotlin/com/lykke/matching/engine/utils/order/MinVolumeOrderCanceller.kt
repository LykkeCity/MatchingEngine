package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.NotEnoughFundsLimitOrderCancelParams
import org.apache.log4j.Logger
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MinVolumeOrderCanceller(private val assetsPairsHolder: AssetsPairsHolder,
                              private val balancesHolder: BalancesHolder,
                              private val genericLimitOrderService: GenericLimitOrderService,
                              private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                              private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                              private val orderBookQueue: BlockingQueue<OrderBook>,
                              private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>) {

    companion object {
        private val LOGGER = Logger.getLogger(MinVolumeOrderCanceller::class.java.name)

        private fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    fun cancel() {
        teeLog("Starting order books analyze to cancel min volume orders")

        val now = Date()
        val ordersToCancel = HashMap<String, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
        var totalCount = 0
        val checkAndAddToCancel: (order: NewLimitOrder) -> Unit = { order ->
            val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
            if (assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume) {
                teeLog("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} min volume is ${assetPair.minVolume}, remaining volume is ${order.getAbsRemainingVolume()}")
                ordersToCancel.getOrPut(order.assetPairId) { HashMap() }.getOrPut(order.isBuySide()) { LinkedList() }.add(order)
                totalCount++
            }
        }

        genericLimitOrderService.getAllOrderBooks().values.forEach {
            val orderBook = it.copy()
            orderBook.getOrderBook(true).forEach { order -> checkAndAddToCancel(order) }
            orderBook.getOrderBook(false).forEach { order -> checkAndAddToCancel(order) }
        }


        teeLog("Starting orders cancellation (orders count: $totalCount)")
        val walletOperations = LinkedList<WalletOperation>()
        val clientsLimitOrdersReport = LimitOrdersReport()
        val trustedClientsLimitOrdersReport = LimitOrdersReport()
        ordersToCancel.forEach { assetPairId, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                val orderBook = genericLimitOrderService.getOrderBook(assetPairId).copy()
                orders.forEach { order -> orderBook.removeOrder(order) }
                genericLimitOrderService.setOrderBook(assetPairId, orderBook)
                val cancelResult = genericLimitOrderService.cancelNotEnoughFundsOrder(NotEnoughFundsLimitOrderCancelParams(orders))
                walletOperations.addAll(cancelResult.walletOperation)
                trustedClientsLimitOrdersReport.orders.addAll(cancelResult.trustedClientLimitOrderWithTrades)
                clientsLimitOrdersReport.orders.addAll(cancelResult.clientLimitOrderWithTrades)
                genericLimitOrderService.updateOrderBook(assetPairId, isBuy)

                val rabbitOrderBook = OrderBook(assetPairId, isBuy, now, genericLimitOrderService.getOrderBook(assetPairId).copy().getOrderBook(isBuy))
                orderBookQueue.put(rabbitOrderBook)
                rabbitOrderBookQueue.put(rabbitOrderBook)
            }
        }

        if (clientsLimitOrdersReport.orders.isNotEmpty()) {
            clientsLimitOrdersQueue.put(clientsLimitOrdersReport)
        }
        if (trustedClientsLimitOrdersReport.orders.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(trustedClientsLimitOrdersReport)
        }

        teeLog("Starting balances updating (wallet operations count: ${walletOperations.size})")
        balancesHolder.processWalletOperations(UUID.randomUUID().toString(), MessageType.LIMIT_ORDER.name, walletOperations)

        teeLog("Min volume orders cancellation is finished")
    }
}
