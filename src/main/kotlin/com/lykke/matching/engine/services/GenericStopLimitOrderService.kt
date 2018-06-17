package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.utils.logging.ThrottlingLogger
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap

class GenericStopLimitOrderService(stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor,
                                   private val genericLimitOrderService: GenericLimitOrderService,
                                   private val persistenceManager: PersistenceManager): AbstractGenericLimitOrderService<AssetStopOrderBook> {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GenericLimitOrderService::class.java.name)
    }

    val initialStopOrdersCount: Int
    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, NewLimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<NewLimitOrder>>()

    init {
        val stopOrders = stopOrderBookDatabaseAccessor.loadStopLimitOrders()
        stopOrders.forEach { order ->
            getOrderBook(order.assetPairId).addOrder(order)
            addOrder(order)
        }
        initialStopOrdersCount = stopOrders.size
    }

    private fun addOrder(order: NewLimitOrder) {
        stopLimitOrdersMap[order.externalId] = order
        clientStopLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    fun addOrders(orders: List<NewLimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun searchOrders(clientId: String, assetPair: String, isBuy: Boolean): List<NewLimitOrder> {
        val ordersToRemove = LinkedList<NewLimitOrder>()
        clientStopLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == assetPair && limitOrder.isBuySide() == isBuy) {
                ordersToRemove.add(limitOrder)
            }
        }
        return ordersToRemove
    }

    fun cancelStopLimitOrders(assetPairId: String, orders: Collection<NewLimitOrder>) {
        val orderBook = getOrderBook(assetPairId)
        orders.forEach { order ->
            val uid = order.externalId
            stopLimitOrdersMap.remove(uid)
            removeFromClientMap(uid, clientStopLimitOrdersMap)
            orderBook.removeOrder(order)
            order.status = OrderStatus.Cancelled.name
        }
    }

    override fun cancelLimitOrders(orders: Collection<NewLimitOrder>) {
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

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetStopOrderBook){
        stopLimitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun addStopOrder(order: NewLimitOrder) {
        getOrderBook(order.assetPairId).addOrder(order)
        addOrder(order)
    }

    fun getStopOrderForProcess(assetPairId: String): NewLimitOrder? {
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        return getStopOrderForProcess(assetPairId, orderBook.getBidPrice(), false)
                ?: getStopOrderForProcess(assetPairId, orderBook.getAskPrice(), true)
    }

    private fun getStopOrderForProcess(assetPairId: String, price: Double, isBuySide: Boolean): NewLimitOrder? {
        if (price <= 0) {
            return null
        }
        val stopOrderBook = getOrderBook(assetPairId)
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
            val newStopOrderBook = stopOrderBook.getOrderBook(order.isBuySide()).toMutableList()
            newStopOrderBook.remove(order)
            val updated = persistenceManager.persist(PersistenceData(null,
                    null,
                    null,
                    OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId,
                            order.isBuySide(),
                            newStopOrderBook)),
                            emptyList(),
                            listOf(order))))
            if (!updated) {
                LOGGER.error("Unable to save stop order book")
                return null
            }
            stopLimitOrdersMap.remove(order.externalId)
            removeFromClientMap(order.externalId, clientStopLimitOrdersMap)
            stopOrderBook.removeOrder(order)
            order.price = orderPrice!!
            order.status = OrderStatus.InOrderBook.name
        }
        return order
    }

    private fun removeFromClientMap(uid: String, clientLimitOrdersMap: MutableMap<String, MutableList<NewLimitOrder>>): Boolean {
        val order: NewLimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }?.firstOrNull { it.externalId == uid }
                ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

}
