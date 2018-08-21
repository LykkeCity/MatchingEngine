package com.lykke.matching.engine.web.dto

import java.math.BigDecimal

data class OrderDto(val id: String,
                    val clientId: String,
                    val volume: BigDecimal,
                    val lowerLimitPrice: BigDecimal?,
                    val lowerPrice: BigDecimal?,
                    val upperLimitPrice: BigDecimal?,
                    val upperPrice: BigDecimal?)