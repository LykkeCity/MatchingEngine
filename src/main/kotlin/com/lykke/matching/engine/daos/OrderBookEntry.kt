package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.*

interface OrderBookEntry {
    fun getOrderPrice(): BigDecimal
    fun getCreationDate(): Date
}