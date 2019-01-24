package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.transaction.CurrentTransactionStopOrderBooksHolder
import org.springframework.stereotype.Component
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

@Component
class GenericStopLimitOrderService(private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                   private val expiryOrdersQueue: ExpiryOrdersQueue) : AbstractGenericLimitOrderService<AssetStopOrderBook>() {

    var initialStopOrdersCount = 0
    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, LimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    init {
        update()
    }

    fun update() {
        stopLimitOrdersMap.values.forEach {
            expiryOrdersQueue.removeIfOrderHasExpiryTime(it)
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
        expiryOrdersQueue.addIfOrderHasExpiryTime(order)
        clientStopLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    override fun addOrders(orders: Collection<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    override fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus?, date: Date?) {
        orders.forEach { order ->
            val removedOrder = stopLimitOrdersMap.remove(order.externalId)
            clientStopLimitOrdersMap[order.clientId]?.remove(removedOrder)
            expiryOrdersQueue.removeIfOrderHasExpiryTime(order)
            if (removedOrder != null && status != null) {
                removedOrder.updateStatus(status, date!!)
            }
        }
    }

    override fun getOrderBook(assetPairId: String) = stopLimitOrdersQueues.getOrPut(assetPairId) { AssetStopOrderBook(assetPairId) }!!

    fun getOrder(uid: String) = stopLimitOrdersMap[uid]

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetStopOrderBook) {
        stopLimitOrdersQueues[assetPairId] = assetOrderBook
    }

    override fun getLimitOrdersByClientIdMap() = clientStopLimitOrdersMap

    override fun getOrderBooksByAssetPairIdMap() = stopLimitOrdersQueues

    fun createCurrentTransactionOrderBooksHolder() = CurrentTransactionStopOrderBooksHolder(this)

}
