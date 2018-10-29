package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.exception.MatchingEngineException

class ValidationException(val validationType: Validation = Validation.GENERIC_VALIDATION_FAILURE, validationMessage: String? = null) : MatchingEngineException(validationMessage ?: validationType.message) {
    enum class Validation(val message: String) {
        NEGATIVE_OVERDRAFT_LIMIT("Overdraft limit can not be negative"),
        INVALID_VOLUME_ACCURACY("Invalid volume accuracy"),
        INVALID_PRICE_ACCURACY("Invalid price accuracy"),
        LOW_BALANCE("Low balance"),
        DISABLED_ASSET("Disabled asset"),
        INVALID_FEE("Invalid fee"),
        RESERVED_VOLUME_HIGHER_THAN_BALANCE("Reserved volume higher than balance"),
        NO_LIQUIDITY("No liquidity"),
        TOO_SMALL_VOLUME("Too small volume"),
        UNKNOWN_ASSET("Unknown asset"),
        BALANCE_LOWER_THAN_RESERVED("Balance lower than reserved"),
        LIMIT_ORDER_NOT_FOUND("Limit order not found"),
        NOT_ACCEPTABLE_MESSAGE_SWITCH_SETTING_VALUE("Supplied value is not supported")
    }
}