package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.LimitOrder

class ProcessedOrder(val order: LimitOrder, val accepted: Boolean, val reason: String? = null)