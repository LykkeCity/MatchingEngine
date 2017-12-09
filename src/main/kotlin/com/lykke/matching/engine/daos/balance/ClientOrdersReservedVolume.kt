package com.lykke.matching.engine.daos.balance

import java.util.LinkedList

data class ClientOrdersReservedVolume(var volume: Double = 0.0, val orderIds: MutableList<String> = LinkedList())