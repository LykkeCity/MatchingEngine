package com.lykke.matching.engine.order.process.context

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.order.transaction.ExecutionContext
import java.math.BigDecimal

class MarketOrderExecutionContext(order: MarketOrder,
                                  lowerMidPriceBound: BigDecimal?,
                                  upperMidPriceBound: BigDecimal?, executionContext: ExecutionContext)
    : OrderExecutionContext<MarketOrder>(order, lowerMidPriceBound, upperMidPriceBound, executionContext)