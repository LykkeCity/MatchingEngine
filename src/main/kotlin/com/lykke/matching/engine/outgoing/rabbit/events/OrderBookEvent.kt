package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.outgoing.messages.OrderBook

class OrderBookEvent(val orderBook: OrderBook)