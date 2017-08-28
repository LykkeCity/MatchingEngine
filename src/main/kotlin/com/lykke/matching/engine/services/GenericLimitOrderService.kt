package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

class GenericLimitOrderService(private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor,
                               private val assetsPairsHolder: AssetsPairsHolder,
                               private val balancesHolder: BalancesHolder,
                               private val tradesInfoQueue: BlockingQueue<TradeInfo>,
                               private val quotesNotificationQueue: BlockingQueue<QuotesUpdate>) {

    companion object {
        val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
    }

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, NewLimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<NewLimitOrder>>()

    init {
        val orders = orderBookDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
    }

    fun addToOrderBook(order: NewLimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        addOrder(order)
    }

    fun processLimitOrder(order: NewLimitOrder) {
        updateOrderBook(order.assetPairId, order.isBuySide())
    }

    fun addOrder(order: NewLimitOrder) {
        limitOrdersMap.put(order.externalId, order)
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList<NewLimitOrder>() }.add(order)
        quotesNotificationQueue.put(QuotesUpdate(order.assetPairId, order.price, order.volume))
    }

    fun addOrders(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun updateLimitOrder(order: NewLimitOrder) {
        updateOrderBook(order.assetPairId, order.isBuySide())
    }

    fun moveOrdersToDone(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(order)
        }
    }

    fun getAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean): List<NewLimitOrder> {
        val ordersToRemove = LinkedList<NewLimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                ordersToRemove.add(limitOrder)
            }
        }
        clientLimitOrdersMap[clientId]?.removeAll(ordersToRemove)
        return ordersToRemove
    }

    fun cancelAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean) {
        val ordersToRemove = LinkedList<NewLimitOrder>()
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

    fun setOrderBook(assetPair: String, isBuy: Boolean, book: PriorityBlockingQueue<NewLimitOrder>){
        limitOrdersQueues.getOrPut(assetPair) { AssetOrderBook(assetPair) }.setOrderBook(isBuy, book)
    }

    fun isEnoughFunds(order: NewLimitOrder, volume: Double): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)

        return if (order.isBuySide()) {
            val availableBalance = balancesHolder.getAvailableReservedBalance(order.clientId, assetPair.quotingAssetId)
            LOGGER.debug("${order.clientId} ${assetPair.quotingAssetId} : ${RoundingUtils.roundForPrint(availableBalance)} >= ${RoundingUtils.roundForPrint(volume * order.price)}")
            availableBalance >= volume * order.price
        } else {
            val availableBalance = balancesHolder.getAvailableReservedBalance(order.clientId, assetPair.baseAssetId)
            LOGGER.debug("${order.clientId} ${assetPair.baseAssetId} : ${RoundingUtils.roundForPrint(availableBalance)} >= ${RoundingUtils.roundForPrint(volume)}")
            availableBalance >= volume
        }
    }

    fun cancelLimitOrder(uid: String): NewLimitOrder? {
        val order = limitOrdersMap.remove(uid) ?: return null

        getOrderBook(order.assetPairId).removeOrder(order)
        order.status = Cancelled.name
        updateOrderBook(order.assetPairId, order.isBuySide())
        return order
    }

    fun cancelLimitOrders(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            val ord = limitOrdersMap.remove(order.externalId)
            if (ord != null) {
                ord.status = Cancelled.name
            }
        }
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
