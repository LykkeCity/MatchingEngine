package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LimitOrder

class UpdatedOrders(val fullOrderBook: Collection<LimitOrder>,
                    val updatedOrder: LimitOrder?)