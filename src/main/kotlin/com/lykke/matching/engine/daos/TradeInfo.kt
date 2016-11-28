package com.lykke.matching.engine.daos

import java.util.Date

data class TradeInfo(val assetPair: String, val isBuy: Boolean, val price: Double, val date: Date)