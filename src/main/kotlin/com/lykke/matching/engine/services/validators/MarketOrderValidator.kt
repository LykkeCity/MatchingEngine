package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import java.util.concurrent.PriorityBlockingQueue


interface MarketOrderValidator {
    fun performValidation(order: MarketOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>)
}