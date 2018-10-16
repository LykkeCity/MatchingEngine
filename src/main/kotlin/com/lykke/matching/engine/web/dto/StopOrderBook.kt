package com.lykke.matching.engine.web.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

data class StopOrderBook(val assetPair: String,
                         @get:JsonProperty("isBuy")
                         @JsonProperty("isBuy")
                         val isBuy: Boolean,

                         @get:JsonProperty("isLower")
                         @JsonProperty("isLower")
                         val isLower: Boolean,

                         val timestamp: Date,
                         val prices: List<StopOrder> )