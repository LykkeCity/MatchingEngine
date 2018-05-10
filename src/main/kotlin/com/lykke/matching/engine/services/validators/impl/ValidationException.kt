package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.exception.MatchingEngineException
import org.apache.commons.lang3.StringUtils

class ValidationException(val validationType: Validation, message: String = StringUtils.EMPTY): MatchingEngineException(message) {
    enum class Validation {
        INVALID_VOLUME_ACCURACY,
        LOW_BALANCE,
        DISABLED_ASSET,
        INVALID_FEE
    }
}