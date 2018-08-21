package com.lykke.matching.engine.web.dto

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import java.util.*

data class StopOrderBookDto(val assetPair: String,
                            val isBuy: Boolean,
                            val isLower: Boolean,
                            val timestamp: Date,
                            val prices: List<OrderDto> ) : JsonSerializable()