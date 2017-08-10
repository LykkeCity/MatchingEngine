package com.lykke.matching.engine.outgoing.messages

import java.util.LinkedList

class LimitOrdersReport (
    val orders: MutableList<LimitOrderWithTrades> = LinkedList()
): JsonSerializable()