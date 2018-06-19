package com.lykke.matching.engine.notification

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper
import org.apache.log4j.Logger
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.LinkedList
import java.util.concurrent.CopyOnWriteArrayList

@Component
class BalanceUpdateHandler {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateHandler::class.java.name)
    }

    private val connections = CopyOnWriteArrayList<ClientHandler>()

    @EventListener
    fun processBalanceUpdate(event: BalanceUpdateNotificationEvent) {
            if (connections.size > 0) {
                val protoNotification = ProtocolMessages.BalanceNotification
                        .newBuilder()
                        .setClientId(event.balanceUpdateNotification.clientId)
                        .build()

                val disconnected = LinkedList<ClientHandler>()
                connections.forEach {
                    try {
                        it.writeOutput(ByteHelper.toByteArray(MessageType.BALANCE_UPDATE_NOTIFICATION.type, protoNotification.serializedSize, protoNotification.toByteArray()))
                    } catch (exception: Exception){
                        disconnected.add(it)
                        LOGGER.info("Removed holders notification subscription from ${it.clientHostName}")
                    }
                }
                if (disconnected.size > 0) {
                    connections.removeAll(disconnected)
                }
            }
    }

    fun subscribe(handler: ClientHandler) {
        connections.add(handler)
        LOGGER.info("Added holders notification subscription from ${handler.clientHostName}")
    }
}