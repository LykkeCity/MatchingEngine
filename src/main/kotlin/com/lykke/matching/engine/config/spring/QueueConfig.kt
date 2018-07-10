package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class QueueConfig {

    @Bean
    open fun clientsEventsQueue(): BlockingQueue<Event<*>> {
        return LinkedBlockingQueue()
    }

    @Bean
    open fun trustedClientsEventsQueue(): BlockingQueue<ExecutionEvent> {
        return LinkedBlockingQueue()
    }
}