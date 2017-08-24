package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.NewLimitOrder
import java.util.LinkedList

class LimitOrderWithTrades (
        val order: NewLimitOrder,
        val trades: MutableList<LimitTradeInfo> = LinkedList()
): JsonSerializable()