package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.outgoing.messages.OrderBook

class RabbitorderBookEvent(val orderBook: OrderBook)