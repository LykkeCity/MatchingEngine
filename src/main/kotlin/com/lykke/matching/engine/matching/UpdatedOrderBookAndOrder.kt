package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LimitOrder

class UpdatedOrderBookAndOrder(val updatedOrderBook: Collection<LimitOrder>,
                               val updatedOrder: LimitOrder?)