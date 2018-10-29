package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.LimitOrder
import java.util.LinkedList

class LimitOrderWithTrades (
        val order: LimitOrder,
        val trades: MutableList<LimitTradeInfo> = LinkedList()
)