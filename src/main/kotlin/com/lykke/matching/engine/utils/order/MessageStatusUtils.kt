package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.impl.ValidationException.Validation.*

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
                OrderStatus.InvalidVolume -> MessageStatus.INVALID_VOLUME
                OrderStatus.InvalidValue -> MessageStatus.INVALID_ORDER_VALUE
                OrderStatus.TooHighPriceDeviation -> MessageStatus.TOO_HIGH_PRICE_DEVIATION
                OrderStatus.TooHighMidPriceDeviation -> MessageStatus.TOO_HIGH_MID_PRICE_DEVIATION
            }
        }

        fun toMessageStatus(validationErrorType: ValidationException.Validation): MessageStatus {
            return when (validationErrorType) {
                DISABLED_ASSET -> MessageStatus.DISABLED_ASSET
                INVALID_VOLUME_ACCURACY -> MessageStatus.INVALID_VOLUME_ACCURACY
                INVALID_PRICE_ACCURACY -> MessageStatus.INVALID_PRICE_ACCURACY
                LOW_BALANCE -> MessageStatus.LOW_BALANCE
                INVALID_FEE -> MessageStatus.INVALID_FEE
                RESERVED_VOLUME_HIGHER_THAN_BALANCE -> MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE
                NO_LIQUIDITY -> MessageStatus.NO_LIQUIDITY
                TOO_SMALL_VOLUME -> MessageStatus.TOO_SMALL_VOLUME
                UNKNOWN_ASSET -> MessageStatus.UNKNOWN_ASSET
                BALANCE_LOWER_THAN_RESERVED -> MessageStatus.BALANCE_LOWER_THAN_RESERVED
                NEGATIVE_OVERDRAFT_LIMIT -> MessageStatus.NEGATIVE_OVERDRAFT_LIMIT
                GENERIC_VALIDATION_FAILURE -> MessageStatus.BAD_REQUEST
                LIMIT_ORDER_NOT_FOUND -> MessageStatus.LIMIT_ORDER_NOT_FOUND
            }
        }
    }
}