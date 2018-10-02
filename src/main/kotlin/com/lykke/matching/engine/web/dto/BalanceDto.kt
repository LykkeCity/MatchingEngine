package com.lykke.matching.engine.web.dto

import java.math.BigDecimal

class BalanceDto(val assetId: String, val balance: BigDecimal, val reservedBalance: BigDecimal)