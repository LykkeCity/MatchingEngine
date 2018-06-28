package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades

class RabbitSwapEvent (val marketOrderWithTrades: MarketOrderWithTrades)