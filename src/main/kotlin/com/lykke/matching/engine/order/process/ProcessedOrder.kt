package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.NewLimitOrder

class ProcessedOrder(val order: NewLimitOrder, val accepted: Boolean, val reason: String? = null)