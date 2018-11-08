package com.lykke.matching.engine.order.process.context

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.order.transaction.ExecutionContext

class MarketOrderExecutionContext(order: MarketOrder,
                                  executionContext: ExecutionContext)
    : OrderExecutionContext<MarketOrder>(order, executionContext)