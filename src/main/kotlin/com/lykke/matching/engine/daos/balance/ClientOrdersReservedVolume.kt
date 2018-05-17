package com.lykke.matching.engine.daos.balance

import java.math.BigDecimal
import java.util.LinkedList

data class ClientOrdersReservedVolume(var volume: BigDecimal = BigDecimal.ZERO, val orderIds: MutableList<String> = LinkedList())