package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.events.OrderBookEvent
import org.springframework.context.event.EventListener

class TestOrderBookListener : AbstractEventListener<OrderBookEvent, OrderBook>() {
    override fun extract(t: OrderBookEvent): OrderBook {
        return t.orderBook
    }

    @EventListener
    override fun process(event: OrderBookEvent) {
        super.process(event)
    }
}