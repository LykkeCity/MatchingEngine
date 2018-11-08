package com.lykke.matching.engine.common.events

import com.lykke.matching.engine.order.transaction.ExecutionContext
import java.math.BigDecimal

class RefMidPriceDangerousChangeEvent(val assetPairId: String, val refMidPrice: BigDecimal, val executionContext: ExecutionContext)