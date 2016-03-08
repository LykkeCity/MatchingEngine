package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import org.apache.log4j.Logger
import java.util.Comparator
import java.util.Date
import java.util.HashMap
import java.util.PriorityQueue

class LimitOrderService(private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor,
                        private val cashOperationService: CashOperationService): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderService::class.java.name)

        val SELL_COMPARATOR = Comparator<LimitOrder>({ o1, o2 ->
            o1.price.compareTo(o2.price)
        })

        val BUY_COMPARATOR = Comparator<LimitOrder>({ o1, o2 ->
            o2.price.compareTo(o1.price)
        })
    }
    private val limitOrdersQueues = HashMap<String, PriorityQueue<LimitOrder>>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()

    init {
        val orders = limitOrderDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        LOGGER.info("Loaded ${orders.size} limit orders on startup.")
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got limit order id: ${message.uid}, client ${message.clientId}, asset: ${message.assetPairId}, volume: ${message.volume}, price: ${message.price}")

        val order = LimitOrder(
                uid = message.uid.toString(),
                assetPairId = message.assetPairId,
                clientId = message.clientId,
                price = message.price,
                createdAt = Date(message.timestamp),
                registered = Date(),
                status = OrderStatus.InOrderBook.name,
                volume = message.volume,
                remainingVolume = message.volume
        )

        addToOrderBook(order)
        limitOrderDatabaseAccessor.addLimitOrder(order)
        LOGGER.debug("Limit order id: ${message.uid}, client ${message.clientId}, asset: ${message.assetPairId}, volume: ${message.volume}, price: ${message.price} added to order book")
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.partitionKey) {
            PriorityQueue<LimitOrder>(if (order.isBuySide()) LimitOrderService.BUY_COMPARATOR else LimitOrderService.SELL_COMPARATOR)
        }
        orderBook.add(order)
        limitOrdersMap.put(order.getId(), order)
    }

    fun updateLimitOrder(order: LimitOrder) {
        limitOrderDatabaseAccessor.updateLimitOrder(order)
    }

    fun moveOrdersToDone(orders: List<LimitOrder>) {
        limitOrderDatabaseAccessor.addLimitOrdersDone(orders)
        limitOrderDatabaseAccessor.deleteLimitOrders(orders)
        orders.forEach { limitOrdersMap.remove(it.getId()) }
    }

    fun getOrderBook(key: String) = limitOrdersQueues[key]

    fun isEnoughFunds(order: LimitOrder, volume: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPairId) ?: return false

        if (order.isBuySide()) {
            return cashOperationService.getBalance(order.clientId, assetPair.quotingAssetId) >= volume * order.price
        } else {
            return cashOperationService.getBalance(order.clientId, assetPair.baseAssetId) >= volume
        }
    }

    fun cancelLimitOrder(uid: String) {
        val order = limitOrdersMap.remove(uid)
        if (order == null) {
            LOGGER.debug("Unable to cancel order $uid: missing order or already processed")
            return
        }

        getOrderBook(Order.buildPartitionKey(order.assetPairId, order.getSide()))?.remove(order)
        order.status = Cancelled.name
        val list = listOf(order)
        limitOrderDatabaseAccessor.addLimitOrdersDone(list)
        limitOrderDatabaseAccessor.deleteLimitOrders(list)
        LOGGER.debug("Order $uid cancelled")
    }
}
