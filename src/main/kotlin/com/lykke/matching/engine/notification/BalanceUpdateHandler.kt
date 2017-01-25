package com.lykke.matching.engine.notification

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper
import org.apache.log4j.Logger
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CopyOnWriteArrayList

class BalanceUpdateHandler(private val notificationQueue: BlockingQueue<BalanceUpdateNotification>): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateHandler::class.java.name)
    }

    private val connections = CopyOnWriteArrayList<ClientHandler>()

    override fun run() {
        while (true) {
            val notification = notificationQueue.take()
            if (connections.size > 0) {
                val protoNotification = ProtocolMessages.BalanceNotification.newBuilder().setClientId(notification.clientId).build()
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
    }

    fun subscribe(handler: ClientHandler) {
        connections.add(handler)
        LOGGER.info("Added holders notification subscription from ${handler.clientHostName}")
    }
}