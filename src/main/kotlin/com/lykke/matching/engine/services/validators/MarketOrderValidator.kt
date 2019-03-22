package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.services.AssetOrderBook

interface MarketOrderValidator {
    fun performValidation(order: MarketOrder,
                          orderBook: AssetOrderBook,
                          feeInstruction: FeeInstruction?,
                          feeInstructions: List<NewFeeInstruction>?)
}