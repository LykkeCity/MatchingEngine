package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.NewLimitOrder

class UpdatedOrders(val fullOrderBook: Collection<NewLimitOrder>,
                    val updatedOrders: Collection<NewLimitOrder>)