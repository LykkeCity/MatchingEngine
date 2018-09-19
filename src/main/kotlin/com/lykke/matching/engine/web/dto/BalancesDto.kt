package com.lykke.matching.engine.web.dto

import java.math.BigDecimal

class BalancesDto(val assetId: String, val balance: BigDecimal, val reservedBalance: BigDecimal)