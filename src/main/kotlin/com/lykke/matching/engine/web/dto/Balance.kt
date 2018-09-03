package com.lykke.matching.engine.web.dto

import java.math.BigDecimal

class Balance(val assetId: String, balance: BigDecimal?, reservedBalance: BigDecimal?) {
    val balance: BigDecimal = balance ?: BigDecimal.ZERO
    val reservedBalance: BigDecimal = reservedBalance ?: BigDecimal.ZERO
}