package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import org.apache.log4j.Logger
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class LimitOrderService(private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor,
                        private val cashOperationService: CashOperationService): AbsractService<ProtocolMessages.LimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderService::class.java.name)

        private val ORDER_ID = "OrderId"
    }

    //asset -> side -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
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
                uid = UUID.randomUUID().toString(),
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
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        limitOrdersMap.put(order.getId(), order)
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

        getOrderBook(order.assetPairId)?.removeOrder(order)
        order.status = Cancelled.name
        limitOrderDatabaseAccessor.addLimitOrderDone(order)
        limitOrderDatabaseAccessor.deleteLimitOrders(listOf(order))
        LOGGER.debug("Order $uid cancelled")
    }

    fun buildMarketProfile(): List<BestPrice> {
        val result = LinkedList<BestPrice>()

        limitOrdersQueues.values.forEach { book ->
            result.add(BestPrice(book.assetId, ask = book.getAskPrice(), bid = book.getBidPrice()))
        }

        return result
    }
}
