package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

data class TradeInfo(val assetPair: String, val isBuy: Boolean, val price: BigDecimal, val date: Date)