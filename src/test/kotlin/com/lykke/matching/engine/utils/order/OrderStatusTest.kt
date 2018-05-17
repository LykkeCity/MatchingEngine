package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus
import org.junit.Assert.assertEquals
import org.junit.Test

class OrderStatusTest {

    @Test
    fun testOrderStatusConvertToMessageStatus() {
        val messageStatus = MessageStatusUtils.toMessageStatus(OrderStatus.ReservedVolumeGreaterThanBalance)

        assertEquals(MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, messageStatus)
    }
}