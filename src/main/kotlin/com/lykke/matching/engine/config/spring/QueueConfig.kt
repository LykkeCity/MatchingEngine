package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.outgoing.messages.v2.AbstractEvent
import com.lykke.matching.engine.outgoing.messages.v2.ExecutionEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
class QueueConfig {

    @Bean
    open fun clientsEventsQueue(): BlockingQueue<AbstractEvent<*>> {
        return LinkedBlockingQueue()
    }

    @Bean
    open fun trustedClientsEventsQueue(): BlockingQueue<ExecutionEvent> {
        return LinkedBlockingQueue()
    }
}