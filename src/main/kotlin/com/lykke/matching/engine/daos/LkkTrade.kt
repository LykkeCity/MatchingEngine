package com.lykke.matching.engine.daos

import java.util.Date

data class LkkTrade(val assetPair: String, val price: Double, val volume: Double, val date: Date)