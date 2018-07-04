package com.lykke.matching.engine.services

import com.lykke.matching.engine.outgoing.messages.v2.AbstractEvent
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class MessageSender(private val clientsEventsQueue: BlockingQueue<AbstractEvent<*>>,
                    private val trustedClientsEventsQueue: BlockingQueue<AbstractEvent<*>>) {

    fun sendTrustedClientsMessage(message: AbstractEvent<*>) {
        trustedClientsEventsQueue.put(message)
    }

    fun sendMessage(message: AbstractEvent<*>) {
        clientsEventsQueue.put(message)
    }

}