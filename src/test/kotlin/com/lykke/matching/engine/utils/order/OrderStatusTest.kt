package com.lykke.matching.engine.utils.order

import com.google.common.base.CaseFormat
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus
import org.junit.Assert.assertEquals
import org.junit.Test

class OrderStatusTest {

    @Test
    fun testOrderStatusConvertToMessageStatus() {
        val orderStatuses = orderStatusesThatCanBeConverted()

        orderStatuses.forEach { orderStatus: OrderStatus ->
            val messageStatus = OrderStatusUtils.toMessageStatus(orderStatus)
            assertEquals(orderStatusNameToMessageStatus(orderStatus.name), messageStatus.name)
        }
    }

    private fun orderStatusNameToMessageStatus (orderStatus: String): String {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, orderStatus)
    }

    private fun orderStatusesThatCanBeConverted(): List<OrderStatus> {
        val messageStatuses = MessageStatus.values().map { messageStatus -> messageStatus.name }

        return OrderStatus
                .values()
                .filter { orderStatus -> messageStatuses.contains(orderStatusNameToMessageStatus(orderStatus.name))}
    }
}