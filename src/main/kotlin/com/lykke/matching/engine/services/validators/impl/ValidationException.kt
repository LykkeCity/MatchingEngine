package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.exception.MatchingEngineException

class ValidationException(message: String, val validationType: Validation): MatchingEngineException(message) {
    enum class Validation {
        INVALID_VOLUME_ACCURACY,
        LOW_BALANCE,
        DISABLED_ASSET,
        INVALID_FEE
    }
}