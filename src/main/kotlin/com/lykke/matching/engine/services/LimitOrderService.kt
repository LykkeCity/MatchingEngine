package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderSide.Sell
import com.lykke.matching.engine.order.OrderStatus
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
    private val limitOrders = HashMap<String, PriorityQueue<LimitOrder>>()

    init {
        val orders = limitOrderDatabaseAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        LOGGER.info("Loaded ${orders.size} limit orders on startup.")
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        val orderSide = OrderSide.valueOf(message.orderAction)
        LOGGER.debug("Got limit order id: ${message.uid}, client ${message.clientId}, asset: ${message.assetId}, volume: ${message.volume}, price: ${message.price}, side: ${orderSide?.name}")
        if (orderSide == null) {
            LOGGER.error("Unknown order action: ${message.orderAction}")
            return
        }

        val order = LimitOrder(
                partitionKey = "${message.assetId}_${orderSide.name}",
                rowKey = message.uid.toString(),
                assetId = message.assetId,
                clientId = message.clientId,
                lastMatchTime = null,
                blockChain = message.blockChain,
                orderType = orderSide.name,
                price = message.price,
                createdAt = Date(message.timestamp),
                registered = Date(),
                status = OrderStatus.InOrderBook.name,
                volume = message.volume,
                remainingVolume = message.volume,
                matchedOrders = null
        )

        addToOrderBook(order)
        limitOrderDatabaseAccessor.addLimitOrder(order)
        LOGGER.debug("Limit order id: ${message.uid}, client ${message.clientId}, asset: ${message.assetId}, volume: ${message.volume}, price: ${message.price}, side: ${orderSide.name} added to order book")
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrders.getOrPut(order.partitionKey) {
            PriorityQueue<LimitOrder>(if (order.orderType == Buy.name) LimitOrderService.BUY_COMPARATOR else LimitOrderService.SELL_COMPARATOR)
        }
        orderBook.add(order)
    }

    fun updateLimitOrder(order: LimitOrder) {
        limitOrderDatabaseAccessor.updateLimitOrder(order)
    }

    fun moveOrdersToDone(orders: List<LimitOrder>) {
        limitOrderDatabaseAccessor.addLimitOrdersDone(orders)
        limitOrderDatabaseAccessor.deleteLimitOrders(orders)
    }

    fun getOrderBook(key: String) = limitOrders[key]

    fun isEnoughFunds(order: LimitOrder, volume: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPair) ?: return false

        when (OrderSide.valueOf(order.orderType)) {
            Sell -> {
                return cashOperationService.getBalance(order.clientId, assetPair.baseAssetId) >= volume
            }
            Buy -> {
                return cashOperationService.getBalance(order.clientId, assetPair.quotingAssetId) >= volume * order.price
            }
        }
    }
}
