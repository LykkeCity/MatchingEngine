package com.lykke.matching.engine.balance

import java.math.BigDecimal

interface BalancesGetter {
    fun getAvailableBalance(clientId: String, assetId: String): BigDecimal
    fun getAvailableReservedBalance(clientId: String, assetId: String): BigDecimal
    fun getReservedBalance(clientId: String, assetId: String): BigDecimal
}