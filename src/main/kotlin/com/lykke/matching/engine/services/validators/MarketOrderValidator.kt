package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import java.util.concurrent.PriorityBlockingQueue


interface MarketOrderValidator {
    fun performValidation(order: MarketOrder, orderBook: PriorityBlockingQueue<LimitOrder>,
                                   feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?)
}