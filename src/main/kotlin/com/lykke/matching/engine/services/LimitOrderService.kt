package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderStatus
import org.apache.log4j.Logger
import java.util.Comparator
import java.util.Date
import java.util.HashMap
import java.util.PriorityQueue
import java.util.UUID

class LimitOrderService(private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor): AbsractService<ProtocolMessages.LimitOrder> {

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

    override fun processMessage(array: ByteArray) {
        val message = parse(array)
        LOGGER.debug("Got limit order from client ${message.clientId}, asset: ${message.assetId}, volume: ${message.volume}, price: ${message.price}")
        val orderSide = OrderSide.valueOf(message.orderAction)
        if (orderSide == null) {
            LOGGER.error("Unknown order action: ${message.orderAction}")
            return
        }

        val order = LimitOrder(
                partitionKey = "${message.assetId}_${orderSide.name}",
                rowKey = UUID.randomUUID().toString(),
                assetId = message.assetId,
                clientId = message.clientId,
                executed = null,
                isOrderTaken = "",
                blockChain = message.blockChain,
                orderType = orderSide.name,
                price = message.price,
                createdAt = Date(message.timestamp),
                registered = Date(),
                status = OrderStatus.InOrderBook.name,
                volume = message.volume
        )

        addToOrderBook(order)
        limitOrderDatabaseAccessor.addLimitOrder(order)
    }

    private fun parse(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrders.getOrPut(order.partitionKey) {
            PriorityQueue<LimitOrder>(if (order.orderType == Buy.name) LimitOrderService.BUY_COMPARATOR else LimitOrderService.SELL_COMPARATOR)
        }
        orderBook.add(order)
    }
}
