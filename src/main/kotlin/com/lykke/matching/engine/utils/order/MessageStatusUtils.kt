package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.ValidationException

class MessageStatusUtils {
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
                OrderStatus.UnknownAsset -> MessageStatus.UNKNOWN_ASSET
                OrderStatus.NotFoundPrevious -> MessageStatus.NOT_FOUND_PREVIOUS
                OrderStatus.Replaced -> MessageStatus.REPLACED

                OrderStatus.InOrderBook,
                OrderStatus.Processing,
                OrderStatus.Pending,
                OrderStatus.Matched,
                OrderStatus.Cancelled -> MessageStatus.OK
                OrderStatus.InvalidPriceAccuracy -> MessageStatus.INVALID_PRICE_ACCURACY
                OrderStatus.InvalidVolumeAccuracy -> MessageStatus.INVALID_VOLUME_ACCURACY
            }
        }

        fun toMessageStatus(validationErrorType: ValidationException.Validation): MessageStatus {
            return when (validationErrorType) {
                ValidationException.Validation.DISABLED_ASSET -> MessageStatus.DISABLED_ASSET
                ValidationException.Validation.INVALID_VOLUME_ACCURACY -> MessageStatus.INVALID_VOLUME_ACCURACY
                ValidationException.Validation.LOW_BALANCE -> MessageStatus.LOW_BALANCE
                ValidationException.Validation.DISABLED_ASSET -> MessageStatus.DISABLED_ASSET
                ValidationException.Validation.INVALID_FEE -> MessageStatus.INVALID_FEE
            }
        }
    }
}