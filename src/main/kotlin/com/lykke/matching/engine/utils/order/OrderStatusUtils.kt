package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus

class OrderStatusUtils {
    companion object {
        fun toMessageStatus(orderStatus: String): MessageStatus {
            return toMessageStatus(OrderStatus.valueOf(orderStatus))
        }

        fun toMessageStatus(orderStatus: OrderStatus): MessageStatus {
            return when (orderStatus) {
                OrderStatus.InvalidFee -> MessageStatus.INVALID_FEE
                OrderStatus.DisabledAsset -> MessageStatus.DISABLED_ASSET
                OrderStatus.InvalidPrice -> MessageStatus.INVALID_PRICE
                OrderStatus.TooSmallVolume -> MessageStatus.TOO_SMALL_VOLUME
                OrderStatus.LeadToNegativeSpread -> MessageStatus.LEAD_TO_NEGATIVE_SPREAD
                OrderStatus.NoLiquidity -> MessageStatus.NO_LIQUIDITY
                OrderStatus.ReservedVolumeGreaterThanBalance -> MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE
                OrderStatus.NotEnoughFunds -> MessageStatus.NOT_ENOUGH_FUNDS
                OrderStatus.InvalidFee -> MessageStatus.INVALID_FEE
                else -> MessageStatus.OK
            }
        }
    }
}