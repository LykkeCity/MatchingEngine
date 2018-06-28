package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitorderBookEvent
import org.springframework.context.event.EventListener

class TestRabbitOrderBookListener : AbstractEventListener<RabbitorderBookEvent, OrderBook>() {
    override fun extract(t: RabbitorderBookEvent): OrderBook {
        return t.orderBook
    }

    @EventListener
    override fun process(event: RabbitorderBookEvent) {
        super.process(event)
    }
}