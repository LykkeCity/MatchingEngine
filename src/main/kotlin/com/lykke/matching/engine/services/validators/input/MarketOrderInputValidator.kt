package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.context.MarketOrderContext

interface MarketOrderInputValidator {
    fun performValidation(marketOrderContext: MarketOrderContext)
}