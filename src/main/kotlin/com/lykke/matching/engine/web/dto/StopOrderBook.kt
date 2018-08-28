package com.lykke.matching.engine.web.dto

import java.util.*

data class StopOrderBook(val assetPair: String,
                         val isBuy: Boolean,
                         val isLower: Boolean,
                         val timestamp: Date,
                         val prices: List<StopOrder> )