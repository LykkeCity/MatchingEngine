package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.transaction.CurrentTransactionStopOrderBooksHolder
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

@Component
class GenericStopLimitOrderService(private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                   private val expiryOrdersQueue: ExpiryOrdersQueue) : AbstractGenericLimitOrderService<AssetStopOrderBook> {

    var initialStopOrdersCount = 0
    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, LimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    init {
        update()
    }

    fun update() {
        stopLimitOrdersMap.values.forEach {
            expiryOrdersQueue.removeOrder(it)
        }
        stopLimitOrdersQueues.clear()
        stopLimitOrdersMap.clear()
        clientStopLimitOrdersMap.clear()

        val stopOrders = stopOrdersDatabaseAccessorsHolder.primaryAccessor.loadStopLimitOrders()
        stopOrders.forEach { order ->
            getOrderBook(order.assetPairId).addOrder(order)
            addOrder(order)
        }
        initialStopOrdersCount = stopOrders.size
    }

    fun getAllOrderBooks() = stopLimitOrdersQueues

    fun addOrder(order: LimitOrder) {
        stopLimitOrdersMap[order.externalId] = order
        expiryOrdersQueue.addOrder(order)
        clientStopLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    override fun addOrders(orders: Collection<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun searchOrders(clientId: String, assetPair: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = mutableListOf<LimitOrder>()
        clientStopLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if ((assetPair == null || limitOrder.assetPairId == assetPair) && (isBuy == null || limitOrder.isBuySide() == isBuy)) {
                result.add(limitOrder)
            }
        }
        return result
    }

    override fun cancelLimitOrders(orders: Collection<LimitOrder>, date: Date) {
        orders.forEach { order ->
            val ord = stopLimitOrdersMap.remove(order.externalId)
            expiryOrdersQueue.removeOrder(order)
            clientStopLimitOrdersMap[order.clientId]?.remove(order)
            if (ord != null) {
                ord.updateStatus(OrderStatus.Cancelled, date)
            }
        }
    }

    override fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus?, date: Date?) {
        orders.forEach { order ->
            val removedOrder = stopLimitOrdersMap.remove(order.externalId)
            if (removedOrder != null) {
                clientStopLimitOrdersMap[order.clientId]?.remove(removedOrder)
                expiryOrdersQueue.removeOrder(order)
                if (status != null) {
                    removedOrder.updateStatus(status, date!!)
                }
            }
        }
    }

    override fun getOrderBook(assetPairId: String) = stopLimitOrdersQueues.getOrPut(assetPairId) { AssetStopOrderBook(assetPairId) }!!

    fun getOrder(uid: String) = stopLimitOrdersMap[uid]

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetStopOrderBook) {
        stopLimitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun createCurrentTransactionOrderBooksHolder() = CurrentTransactionStopOrderBooksHolder(this)

}
