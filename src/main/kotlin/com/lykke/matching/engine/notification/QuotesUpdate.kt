package com.lykke.matching.engine.notification

import java.math.BigDecimal

data class QuotesUpdate(val asset: String, val price: BigDecimal, val volume: BigDecimal)