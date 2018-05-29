package com.lykke.matching.engine.outgoing.messages

import java.util.LinkedList

class LimitOrdersReport (
        val messageId: String,
        val orders: MutableList<LimitOrderWithTrades> = LinkedList()
): JsonSerializable()