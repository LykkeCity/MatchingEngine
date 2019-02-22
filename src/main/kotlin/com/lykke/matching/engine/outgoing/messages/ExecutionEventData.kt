package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.outgoing.messages.v2.builders.EventData

class ExecutionEventData(val clientBalanceUpdates: List<ClientBalanceUpdate>,
                         val limitOrdersWithTrades: List<LimitOrderWithTrades>,
                         val marketOrderWithTrades: MarketOrderWithTrades?): EventData