package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

class GenericLimitOrderService(private val useFileOrderBook: Boolean,
                               private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor,
                               private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor,
                               private val assetsPairsHolder: AssetsPairsHolder,
                               private val balancesHolder: BalancesHolder,
                               private val tradesInfoQueue: BlockingQueue<TradeInfo>,
                               private val quotesNotificationQueue: BlockingQueue<QuotesUpdate>) {

    companion object {
        val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        private val ORDER_ID = "OrderId"
    }

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    private var messagesCount: Long = 0

    init {
        val orders = if (useFileOrderBook) orderBookDatabaseAccessor.loadLimitOrders() else limitOrderDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
    }

    fun processLimitOrder(order: LimitOrder) {
        if (useFileOrderBook) {
            updateOrderBook(order.assetPairId, order.isBuySide())
        } else {
            limitOrderDatabaseAccessor.addLimitOrder(order)
        }
        putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), order.price, Date()))
    }

    fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        addOrder(order)
    }

    fun addOrder(order: LimitOrder) {
        limitOrdersMap.put(order.externalId, order)
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList<LimitOrder>() }.add(order)
        quotesNotificationQueue.put(QuotesUpdate(order.assetPairId, order.price, order.volume))
    }

    fun addOrders(orders: List<LimitOrder>) {
        orders.forEach { order ->
            limitOrdersMap.put(order.externalId, order)
            clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList<LimitOrder>() }.add(order)
            quotesNotificationQueue.put(QuotesUpdate(order.assetPairId, order.price, order.volume))
        }
        if (useFileOrderBook && orders.isNotEmpty()) {
            val order = orders.first()
            updateOrderBook(order.assetPairId, order.isBuySide())
        } else {
            limitOrderDatabaseAccessor.addLimitOrders(orders)
        }
    }

    fun updateLimitOrder(order: LimitOrder) {
        if (useFileOrderBook) {
            if (order.remainingVolume != order.volume) {
                limitOrderDatabaseAccessor.addLimitOrderDone(order)
                limitOrderDatabaseAccessor.addLimitOrderDoneWithGeneratedRowId(order)
            }
            updateOrderBook(order.assetPairId, order.isBuySide())
        } else {
            limitOrderDatabaseAccessor.updateLimitOrder(order)
        }
    }

    fun moveOrdersToDone(orders: List<LimitOrder>) {
        if (!useFileOrderBook) {
            limitOrderDatabaseAccessor.deleteLimitOrders(orders)
        }
        orders.forEach { order ->
            limitOrderDatabaseAccessor.addLimitOrderDone(order)
            limitOrdersMap.remove(order.externalId)
            limitOrderDatabaseAccessor.addLimitOrderDoneWithGeneratedRowId(order)
        }
    }

    fun getAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean): List<LimitOrder> {
        val ordersToRemove = LinkedList<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                ordersToRemove.add(limitOrder)
            }
        }
        clientLimitOrdersMap[clientId]?.removeAll(ordersToRemove)
        return ordersToRemove
    }

    fun cancelAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean) {
        val ordersToRemove = LinkedList<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                cancelLimitOrder(limitOrder.externalId)
                ordersToRemove.add(limitOrder)
            }
        }
        clientLimitOrdersMap[clientId]?.removeAll(ordersToRemove)
    }

    fun updateOrderBook(asset: String, isBuy: Boolean) {
        orderBookDatabaseAccessor.updateOrderBook(asset, isBuy, getOrderBook(asset).getCopyOfOrderBook(isBuy))
    }

    fun getAllOrderBooks() = limitOrdersQueues

    fun getOrderBook(assetPair: String) = limitOrdersQueues[assetPair] ?: AssetOrderBook(assetPair)

    fun setOrderBook(assetPair: String, book: AssetOrderBook){
        limitOrdersQueues[assetPair] = book
    }

    fun setOrderBook(assetPair: String, isBuy: Boolean, book: PriorityBlockingQueue<LimitOrder>){
        var orderBook = limitOrdersQueues[assetPair]
        if (orderBook == null) {
            orderBook = AssetOrderBook(assetPair)
        }
        orderBook.setOrderBook(isBuy, book)
    }

    fun isEnoughFunds(order: LimitOrder, volume: Double): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)

        if (order.isBuySide()) {
            val availableBalance = balancesHolder.getAvailableBalance(order.clientId, assetPair.quotingAssetId)
            LOGGER.debug("${order.clientId} ${assetPair.quotingAssetId} : ${RoundingUtils.roundForPrint(availableBalance)} >= ${RoundingUtils.roundForPrint(volume * order.price)}")
            return availableBalance >= volume * order.price
        } else {
            val availableBalance = balancesHolder.getAvailableBalance(order.clientId, assetPair.baseAssetId)
            LOGGER.debug("${order.clientId} ${assetPair.baseAssetId} : ${RoundingUtils.roundForPrint(availableBalance)} >= ${RoundingUtils.roundForPrint(volume)}")
            return availableBalance >= volume
        }
    }

    fun cancelLimitOrder(uid: String): LimitOrder? {
        val order = limitOrdersMap.remove(uid)
        if (order == null) {
            LOGGER.debug("Unable to cancel order $uid: missing order or already processed")
            return null
        }

        getOrderBook(order.assetPairId).removeOrder(order)
        order.status = Cancelled.name
        limitOrderDatabaseAccessor.addLimitOrderDone(order)
        if (useFileOrderBook) {
            updateOrderBook(order.assetPairId, order.isBuySide())
        } else {
            limitOrderDatabaseAccessor.deleteLimitOrders(listOf(order))
        }
        LOGGER.debug("Order $uid cancelled")
        return order
    }

    fun cancelLimitOrders(orders: List<LimitOrder>) {
        val ordersToCancel = ArrayList<LimitOrder>(orders.size)
        orders.forEach { order ->
            val ord = limitOrdersMap.remove(order.externalId)
            if (ord != null) {
                ord.status = Cancelled.name
                ordersToCancel.add(ord)
            }
        }
        limitOrderDatabaseAccessor.addLimitOrdersDone(ordersToCancel)
        if (useFileOrderBook && orders.isNotEmpty()) {
            val order = orders.first()
            updateOrderBook(order.assetPairId, order.isBuySide())
        } else {
            limitOrderDatabaseAccessor.deleteLimitOrders(ordersToCancel)
        }
    }

    fun removeOrder(uid: String) : LimitOrder? {
        return limitOrdersMap.remove(uid)
    }

    fun putTradeInfo(tradeInfo: TradeInfo) {
        tradesInfoQueue.put(tradeInfo)
    }

    fun buildMarketProfile(): List<BestPrice> {
        val result = LinkedList<BestPrice>()

        limitOrdersQueues.values.forEach { book ->
            val askPrice = book.getAskPrice()
            val bidPrice = book.getBidPrice()
            if (askPrice > 0 || bidPrice > 0) {
                result.add(BestPrice(book.assetId, askPrice, bidPrice))
            }
        }

        return result
    }
}
