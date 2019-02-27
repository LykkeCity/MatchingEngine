package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventData

class ExecutionEventData(val clientBalanceUpdates: List<ClientBalanceUpdate>,
                         val limitOrdersWithTrades: List<LimitOrderWithTrades>,
                         val marketOrderWithTrades: MarketOrderWithTrades?): EventData