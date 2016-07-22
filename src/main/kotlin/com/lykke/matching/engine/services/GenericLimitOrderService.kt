package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap

class GenericLimitOrderService(private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor,
                               private val cashOperationService: CashOperationService,
                               private val tradesInfoQueue: BlockingQueue<TradeInfo>) {

    companion object {
        val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        private val ORDER_ID = "OrderId"
    }

    //asset -> side -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    private var messagesCount: Long = 0

    init {
        val orders = limitOrderDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        LOGGER.info("Loaded ${orders.size} limit orders on startup.")
    }

    fun processLimitOrder(order: LimitOrder) {
        addToOrderBook(order)
        limitOrderDatabaseAccessor.addLimitOrder(order)
        tradesInfoQueue.put(TradeInfo(order.assetPairId, order.isBuySide(), order.price, Date()))
    }

    fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        limitOrdersMap.put(order.getId(), order)
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList<LimitOrder>() }.add(order)
    }

    fun updateLimitOrder(order: LimitOrder) {
        limitOrderDatabaseAccessor.updateLimitOrder(order)
    }

    fun moveOrdersToDone(orders: List<LimitOrder>) {
        limitOrderDatabaseAccessor.deleteLimitOrders(orders)
        orders.forEach { order ->
            order.partitionKey = ORDER_ID
            limitOrderDatabaseAccessor.addLimitOrderDone(order)
            limitOrdersMap.remove(order.getId())
            limitOrderDatabaseAccessor.addLimitOrderDoneWithGeneratedRowId(order)
        }
    }

    fun cancelAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean) {
        val ordersToRemove = LinkedList<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide == isBuy) {
                cancelLimitOrder(limitOrder.getId())
                ordersToRemove.add(limitOrder)
            }
        }
        clientLimitOrdersMap[clientId]?.removeAll(ordersToRemove)
    }

    fun getOrderBook(key: String) = limitOrdersQueues[key]

    fun isEnoughFunds(order: LimitOrder, volume: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPairId) ?: return false

        if (order.isBuySide()) {
            LOGGER.debug("${order.clientId} ${assetPair.quotingAssetId!!} : ${cashOperationService.getBalance(order.clientId, assetPair.quotingAssetId!!)} >= ${volume * order.price}")
            return cashOperationService.getBalance(order.clientId, assetPair.quotingAssetId!!) >= volume * order.price
        } else {
            LOGGER.debug("${order.clientId} ${assetPair.baseAssetId!!} : ${cashOperationService.getBalance(order.clientId, assetPair.baseAssetId!!)} >= $volume")
            return cashOperationService.getBalance(order.clientId, assetPair.baseAssetId!!) >= volume
        }
    }

    fun cancelLimitOrder(uid: String) {
        val order = limitOrdersMap.remove(uid)
        if (order == null) {
            LOGGER.debug("Unable to cancel order $uid: missing order or already processed")
            return
        }

        getOrderBook(order.assetPairId)?.removeOrder(order)
        order.status = Cancelled.name
        limitOrderDatabaseAccessor.addLimitOrderDone(order)
        limitOrderDatabaseAccessor.deleteLimitOrders(listOf(order))
        LOGGER.debug("Order $uid cancelled")
    }

    fun buildMarketProfile(): List<BestPrice> {
        val result = LinkedList<BestPrice>()

        limitOrdersQueues.values.forEach { book ->
            val askPrice = book.getAskPrice()
            val bidPrice = book.getBidPrice()
            if (askPrice > 0 && bidPrice > 0) {
                result.add(BestPrice(book.assetId, askPrice, bidPrice))
            }
        }

        return result
    }
}
