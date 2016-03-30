package com.lykke.matching.engine.daos

import java.util.Date

class TradeInfo(val assetPair: String, val isBuy: Boolean, val price: Double, val date: Date)

class Tick(val openPrice: Double, var closePrice: Double, var highPrice: Double, var lowPrice: Double)