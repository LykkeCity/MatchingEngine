package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import java.math.BigDecimal
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap

class GenericStopLimitOrderService(private val stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor,
                                   private val genericLimitOrderService: GenericLimitOrderService): AbstractGenericLimitOrderService<AssetStopOrderBook> {

    val initialStopOrdersCount: Int
    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, LimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    init {
        val stopOrders = stopOrderBookDatabaseAccessor.loadStopLimitOrders()
        stopOrders.forEach { order ->
            getOrderBook(order.assetPairId).addOrder(order)
            addOrder(order)
        }
        initialStopOrdersCount = stopOrders.size
    }

    private fun addOrder(order: LimitOrder) {
        stopLimitOrdersMap[order.externalId] = order
        clientStopLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    fun addOrders(orders: List<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun searchOrders(clientId: String, assetPair: String, isBuy: Boolean): List<LimitOrder> {
        val ordersToRemove = LinkedList<LimitOrder>()
        clientStopLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                ordersToRemove.add(limitOrder)
            }
        }
        return ordersToRemove
    }

    fun cancelStopLimitOrders(assetPairId: String, isBuy: Boolean, orders: Collection<LimitOrder>) {
        val orderBook = getOrderBook(assetPairId)
        orders.forEach { order ->
            val uid = order.externalId
            stopLimitOrdersMap.remove(uid)
            removeFromClientMap(uid, clientStopLimitOrdersMap)
            orderBook.removeOrder(order)
            order.status = OrderStatus.Cancelled.name
        }
        updateOrderBook(assetPairId, isBuy)
    }

    override fun cancelLimitOrders(orders: Collection<LimitOrder>) {
        orders.forEach { order ->
            val ord = stopLimitOrdersMap.remove(order.externalId)
            clientStopLimitOrdersMap[order.clientId]?.remove(order)
            if (ord != null) {
                ord.status = OrderStatus.Cancelled.name
            }
        }
    }

    override fun getOrderBook(assetPairId: String) = stopLimitOrdersQueues.getOrPut(assetPairId) { AssetStopOrderBook(assetPairId) }!!

    fun getOrder(uid: String) = stopLimitOrdersMap[uid]

    override fun updateOrderBook(assetPairId: String, isBuy: Boolean) {
        stopOrderBookDatabaseAccessor.updateStopOrderBook(assetPairId, isBuy, getOrderBook(assetPairId).getOrderBook(isBuy))
    }

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetStopOrderBook){
        stopLimitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun addStopOrder(order: LimitOrder) {
        getOrderBook(order.assetPairId).addOrder(order)
        addOrder(order)
        updateOrderBook(order.assetPairId, order.isBuySide())
    }

    fun getStopOrderForProcess(assetPairId: String): LimitOrder? {
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        return getStopOrderForProcess(assetPairId, orderBook.getBidPrice(), false)
                ?: getStopOrderForProcess(assetPairId, orderBook.getAskPrice(), true)
    }

    private fun getStopOrderForProcess(assetPairId: String, price: BigDecimal, isBuySide: Boolean): LimitOrder? {
        if (price <= BigDecimal.ZERO) {
            return null
        }
        val stopOrderBook = getOrderBook(assetPairId)
        var order: LimitOrder?
        var orderPrice: BigDecimal? = null
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
            updateOrderBook(order.assetPairId, order.isBuySide())
            order.price = orderPrice!!
            order.status = OrderStatus.InOrderBook.name
        }
        return order
    }

    private fun removeFromClientMap(uid: String, clientLimitOrdersMap: MutableMap<String, MutableList<LimitOrder>>): Boolean {
        val order: LimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }?.firstOrNull { it.externalId == uid }
                ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

}
