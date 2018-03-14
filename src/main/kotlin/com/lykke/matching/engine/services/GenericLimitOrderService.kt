package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
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
                               private val assetsHolder: AssetsHolder,
                               private val assetsPairsHolder: AssetsPairsHolder,
                               private val balancesHolder: BalancesHolder,
                               private val tradesInfoQueue: BlockingQueue<TradeInfo>,
                               private val quotesNotificationQueue: BlockingQueue<QuotesUpdate>,
                               private val applicationSettingsCache: ApplicationSettingsCache) {

    companion object {
        private val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
    }

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, NewLimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<NewLimitOrder>>()
    private val notEnoughFundsLimitOrderCancelService: NotEnoughFundsLimitOrderCancelService = NotEnoughFundsLimitOrderCancelService(this, assetsPairsHolder, balancesHolder, applicationSettingsCache)
    val initialOrdersCount: Int
    val initialStopOrdersCount: Int

    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, NewLimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<NewLimitOrder>>()

    init {
        val orders = orderBookDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        val stopOrders = orderBookDatabaseAccessor.loadStopLimitOrders()
        stopOrders.forEach { order ->
            getStopOrderBook(order.assetPairId).addOrder(order)
            addOrder(order, stopLimitOrdersMap, clientStopLimitOrdersMap)
        }
        initialOrdersCount = orders.size
        initialStopOrdersCount = stopOrders.size
    }

    private fun addToOrderBook(order: NewLimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        addOrder(order)
    }

    private fun addOrder(order: NewLimitOrder, limitOrdersMap: MutableMap<String, NewLimitOrder>, clientLimitOrdersMap: MutableMap<String, MutableList<NewLimitOrder>>) {
        limitOrdersMap[order.externalId] = order
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    fun addOrder(order: NewLimitOrder) {
        addOrder(order, limitOrdersMap, clientLimitOrdersMap)
        quotesNotificationQueue.put(QuotesUpdate(order.assetPairId, order.price, order.volume))
    }

    fun addOrders(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun moveOrdersToDone(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(order)
        }
    }

    fun getAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean): List<NewLimitOrder> {
        return getAllPreviousOrders(clientId, assetPair, isBuy, clientLimitOrdersMap)
    }

    private fun getAllPreviousOrders(clientId: String, assetPair: String, isBuy: Boolean, clientLimitOrdersMap: MutableMap<String, MutableList<NewLimitOrder>>): List<NewLimitOrder> {
        val ordersToRemove = LinkedList<NewLimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                ordersToRemove.add(limitOrder)
            }
        }
        clientLimitOrdersMap[clientId]?.removeAll(ordersToRemove)
        return ordersToRemove
    }

    fun getAllPreviousStopOrders(clientId: String, assetPair: String, isBuy: Boolean): List<NewLimitOrder> {
        return getAllPreviousOrders(clientId, assetPair, isBuy, clientStopLimitOrdersMap)
    }

    fun cancelStopLimitOrder(uid: String, removeFromClientMap: Boolean = false): NewLimitOrder? {
        val order = stopLimitOrdersMap.remove(uid) ?: return null

        if (removeFromClientMap) {
            removeFromClientMap(uid, clientStopLimitOrdersMap)
        }

        getStopOrderBook(order.assetPairId).removeOrder(order)
        order.status = Cancelled.name
        updateStopOrderBook(order.assetPairId, order.isBuySide())
        return order
    }

    fun getStopOrderBook(assetPairId: String) = stopLimitOrdersQueues.getOrPut(assetPairId) { AssetStopOrderBook(assetPairId) }!!

    private fun updateStopOrderBook(assetPairId: String, isBuy: Boolean) {
        orderBookDatabaseAccessor.updateStopOrderBook(assetPairId, isBuy, getStopOrderBook(assetPairId).getOrderBook(isBuy))
    }

    fun addStopOrder(order: NewLimitOrder) {
        addOrder(order, stopLimitOrdersMap, clientStopLimitOrdersMap)
        getStopOrderBook(order.assetPairId).addOrder(order)
        updateStopOrderBook(order.assetPairId, order.isBuySide())
    }

    fun getStopOrderForProcess(assetPairId: String): NewLimitOrder? {
        val orderBook = getOrderBook(assetPairId)
        return getStopOrderForProcess(assetPairId, orderBook.getBidPrice(), false) ?: getStopOrderForProcess(assetPairId, orderBook.getAskPrice(), true)
    }

    private fun getStopOrderForProcess(assetPairId: String, price: Double, isBuySide: Boolean): NewLimitOrder? {
        if (price <= 0) {
            return null
        }
        val stopOrderBook = getStopOrderBook(assetPairId)
        var order: NewLimitOrder?
        var orderPrice: Double? = null
        order = stopOrderBook.getOrder(price, isBuySide, true)
        if (order != null) {
            orderPrice = order.lowerPrice!!
        } else {
            order = stopOrderBook.getOrder(price, isBuySide, false)
            if (order != null) {
                orderPrice = order.upperPrice!!
            }
        }
        if (order != null) {
            stopLimitOrdersMap.remove(order.externalId)
            removeFromClientMap(order.externalId, clientStopLimitOrdersMap)
            stopOrderBook.removeOrder(order)
            updateStopOrderBook(order.assetPairId, order.isBuySide())
            order.price = orderPrice!!
            order.status = OrderStatus.InOrderBook.name
        }
        return order
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

    fun checkAndReduceBalance(order: NewLimitOrder, volume: Double, limitBalances: MutableMap<String, Double>): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val availableBalance = limitBalances[order.clientId] ?: balancesHolder.getAvailableReservedBalance(order.clientId, limitAssetId)
        val accuracy = assetsHolder.getAsset(limitAssetId).accuracy
        val result = availableBalance >= volume
        LOGGER.debug("order=${order.externalId}, client=${order.clientId}, $limitAssetId : ${RoundingUtils.roundForPrint(availableBalance)} >= ${RoundingUtils.roundForPrint(volume)} = $result")
        if (result) {
            limitBalances[order.clientId] = RoundingUtils.parseDouble(availableBalance - volume, accuracy).toDouble()
        }
        return result
    }

    fun cancelLimitOrder(uid: String, removeFromClientMap: Boolean = false): NewLimitOrder? {
        val order = limitOrdersMap.remove(uid) ?: return null
      
        if (removeFromClientMap) {
            removeFromClientMap(uid, clientLimitOrdersMap)
        }
      
        getOrderBook(order.assetPairId).removeOrder(order)
        order.status = Cancelled.name
        updateOrderBook(order.assetPairId, order.isBuySide())
        return order
    }

    private fun removeFromClientMap(uid: String, clientLimitOrdersMap: MutableMap<String, MutableList<NewLimitOrder>>): Boolean {
        val order: NewLimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }?.firstOrNull{it.externalId == uid} ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

    fun cancelLimitOrders(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            val ord = limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(order)
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

    fun cancelNotEnoughFundsOrder(params: NotEnoughFundsLimitOrderCancelParams): NotEnoughFundsLimitOrderCancelResult {
        return notEnoughFundsLimitOrderCancelService.cancelOrder(params)
    }

}
