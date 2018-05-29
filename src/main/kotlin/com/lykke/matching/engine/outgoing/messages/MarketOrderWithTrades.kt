package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.MarketOrder
import java.util.LinkedList

class MarketOrderWithTrades (
    val messageId: String,
    val order: MarketOrder,
    val trades: MutableList<TradeInfo> = LinkedList()
): JsonSerializable()