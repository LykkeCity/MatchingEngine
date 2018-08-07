package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.exception.MatchingEngineException
import org.apache.commons.lang3.StringUtils

class ValidationException(val validationType: Validation = Validation.GENERIC_VALIDATION_FAILURE, message: String = StringUtils.EMPTY): MatchingEngineException(message) {
    enum class Validation {
        GENERIC_VALIDATION_FAILURE,
        INVALID_VOLUME_ACCURACY,
        INVALID_PRICE_ACCURACY,
        LOW_BALANCE,
        DISABLED_ASSET,
        INVALID_FEE,
        RESERVED_VOLUME_HIGHER_THAN_BALANCE,
        NO_LIQUIDITY,
        TOO_SMALL_VOLUME,
        UNKNOWN_ASSET,
        BALANCE_LOWER_THAN_RESERVED,
        LIMIT_ORDER_NOT_FOUND
    }
}