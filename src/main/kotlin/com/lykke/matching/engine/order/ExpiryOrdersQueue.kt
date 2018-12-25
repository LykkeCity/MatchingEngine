package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LimitOrder
import org.springframework.stereotype.Component
import java.util.Date
import java.util.PriorityQueue

@Component
class ExpiryOrdersQueue {

    private val ordersQueue = PriorityQueue<LimitOrder> { o1, o2 ->
        var result = o1.expiryTime!!.compareTo(o2.expiryTime!!)
        if (result == 0) {
            result = o1.createdAt.compareTo(o2.createdAt)
        }
        result
    }

    fun addIfOrderHasExpiryTime(order: LimitOrder): Boolean {
        if (!order.hasExpiryTime()) {
            return false
        }
        return ordersQueue.add(order)
    }

    fun removeIfOrderHasExpiryTime(order: LimitOrder): Boolean {
        if (!order.hasExpiryTime()) {
            return false
        }
        return ordersQueue.remove(order)
    }

    fun getExpiredOrdersExternalIds(date: Date): List<String> {
        val ordersQueueCopy = PriorityQueue<LimitOrder>(ordersQueue)
        if (ordersQueueCopy.isEmpty()) {
            return emptyList()
        }
        val result = mutableListOf<String>()
        while (ordersQueueCopy.peek()?.isExpired(date) == true) {
            result.add(ordersQueueCopy.poll().externalId)
        }
        return result
    }
}