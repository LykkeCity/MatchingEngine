package com.lykke.matching.engine.utils.order

import com.google.common.base.CaseFormat
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus

class OrderStatusUtils {
    companion object {
        fun toMessageStatus(orderStatus: String): MessageStatus {
            return toMessageStatus(OrderStatus.valueOf(orderStatus))
        }

        fun toMessageStatus(orderStatus: OrderStatus): MessageStatus {
            return getStrictMapping(orderStatus) ?: mappingByConvention(orderStatus) ?: MessageStatus.OK
        }

        private fun mappingByConvention(orderStatus: OrderStatus): MessageStatus? {
            val messageStatusName = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, orderStatus.name)

            return MessageStatus.values().findLast { messageStatus -> messageStatus.name.equals(messageStatusName) }
        }

        private fun getStrictMapping(orderStatus: OrderStatus): MessageStatus? {
            return when (orderStatus) {
                OrderStatus.ReservedVolumeGreaterThanBalance -> MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE
                else -> null
            }
        }
    }
}