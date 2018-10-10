package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Component
class GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                   private val genericLimitOrderService: GenericLimitOrderService,
                                   private val persistenceManager: PersistenceManager,
                                   private val expiryOrdersQueue: ExpiryOrdersQueue) : AbstractGenericLimitOrderService<AssetStopOrderBook> {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GenericLimitOrderService::class.java.name)
    }

    final val initialStopOrdersCount: Int
    private val stopLimitOrdersQueues = ConcurrentHashMap<String, AssetStopOrderBook>()
    private val stopLimitOrdersMap = HashMap<String, LimitOrder>()
    private val clientStopLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()

    init {
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

    fun addOrders(orders: List<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun searchOrders(clientId: String, assetPair: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        clientStopLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == (assetPair ?: limitOrder.assetPairId) && limitOrder.isBuySide() == (isBuy ?: limitOrder.isBuySide())) {
                result.add(limitOrder)
            }
        }
        return result
    }

    fun cancelStopLimitOrders(assetPairId: String, orders: Collection<LimitOrder>, date: Date) {
        val orderBook = getOrderBook(assetPairId)
        orders.forEach { order ->
            val uid = order.externalId
            stopLimitOrdersMap.remove(uid)
            expiryOrdersQueue.removeOrder(order)
            removeFromClientMap(uid, clientStopLimitOrdersMap)
            orderBook.removeOrder(order)
            order.updateStatus(OrderStatus.Cancelled, date)
        }
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

    override fun getOrderBook(assetPairId: String) = stopLimitOrdersQueues.getOrPut(assetPairId) { AssetStopOrderBook(assetPairId) }!!

    fun getOrder(uid: String) = stopLimitOrdersMap[uid]

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetStopOrderBook) {
        stopLimitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun addStopOrder(order: LimitOrder) {
        getOrderBook(order.assetPairId).addOrder(order)
        addOrder(order)
    }

    fun getStopOrderForProcess(assetPairId: String, date: Date): LimitOrder? {
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        return getStopOrderForProcess(assetPairId, orderBook.getBidPrice(), false, date)
                ?: getStopOrderForProcess(assetPairId, orderBook.getAskPrice(), true, date)
    }

    private fun getStopOrderForProcess(assetPairId: String, price: BigDecimal, isBuySide: Boolean, date: Date): LimitOrder? {
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
            val newStopOrderBook = stopOrderBook.getOrderBook(order.isBuySide()).toMutableList()
            newStopOrderBook.remove(order)
            val updated = persistenceManager.persist(PersistenceData(null,
                    null,
                    null,
                    OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId,
                            order.isBuySide(),
                            newStopOrderBook)),
                            emptyList(),
                            listOf(order)),
                    null))
            if (!updated) {
                LOGGER.error("Unable to save stop order book")
                return null
            }
            stopLimitOrdersMap.remove(order.externalId)
            expiryOrdersQueue.removeOrder(order)
            removeFromClientMap(order.externalId, clientStopLimitOrdersMap)
            stopOrderBook.removeOrder(order)
            order.price = orderPrice!!
            order.updateStatus(OrderStatus.InOrderBook, date)
        }
        return order
    }

    private fun removeFromClientMap(uid: String, clientLimitOrdersMap: MutableMap<String, MutableList<LimitOrder>>): Boolean {
        val order: LimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }?.firstOrNull { it.externalId == uid }
                ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

}
