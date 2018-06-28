package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.outgoing.rabbit.events.LkkTradesEvent
import org.springframework.context.event.EventListener

class TestLkkTradeListener : AbstractEventListener<LkkTradesEvent, List<LkkTrade>>() {
    override fun extract(t: LkkTradesEvent): List<LkkTrade> {
        return t.trades
    }

    @EventListener
    override fun process(event: LkkTradesEvent) {
        super.process(event)
    }
}