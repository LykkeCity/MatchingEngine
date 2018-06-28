package com.lykke.matching.engine.notification

import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitSwapEvent
import org.springframework.context.event.EventListener

class RabbitSwapListener: AbstractEventListener<RabbitSwapEvent, MarketOrderWithTrades>() {
    override fun extract(t: RabbitSwapEvent): MarketOrderWithTrades {
        return t.marketOrderWithTrades
    }

    @EventListener
    override fun process(event: RabbitSwapEvent) {
        super.process(event)
    }
}