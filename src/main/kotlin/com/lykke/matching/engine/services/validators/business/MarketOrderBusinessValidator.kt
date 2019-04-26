package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.MarketOrder

interface MarketOrderBusinessValidator {
    fun performValidation(order: MarketOrder)
}
