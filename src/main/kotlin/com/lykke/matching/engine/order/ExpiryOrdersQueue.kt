package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LimitOrder
import org.springframework.stereotype.Component
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

@Component
class ExpiryOrdersQueue {

    private val ordersById = ConcurrentHashMap<String, LimitOrder>()

    fun addIfOrderHasExpiryTime(order: LimitOrder): Boolean {
        return if (order.hasExpiryTime()) {
            ordersById[order.id] = order
            true
        } else false
    }

    fun removeIfOrderHasExpiryTime(order: LimitOrder): Boolean {
        return if (order.hasExpiryTime()) {
            ordersById.remove(order.id)
            true
        } else false
    }

    fun getExpiredOrdersExternalIds(date: Date): List<String> {
        return ordersById.values
                .asSequence()
                .filter { it.isExpired(date) }
                .map { it.externalId }
                .toList()
    }
}