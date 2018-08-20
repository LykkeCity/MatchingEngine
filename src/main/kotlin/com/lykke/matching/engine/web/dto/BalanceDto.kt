package com.lykke.matching.engine.web.dto

import java.math.BigDecimal

class BalanceDto(val balance: BigDecimal? = BigDecimal.ZERO, val reservedBalance: BigDecimal? = BigDecimal.ZERO)