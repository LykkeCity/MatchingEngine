package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.NewLimitOrder

class RejectedOrder(val order: NewLimitOrder, val reason: String? = null)