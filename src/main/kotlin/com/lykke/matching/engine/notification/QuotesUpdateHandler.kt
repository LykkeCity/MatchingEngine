package com.lykke.matching.engine.notification

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.socket.ClientHandler
import com.lykke.matching.engine.utils.ByteHelper
import org.apache.log4j.Logger
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CopyOnWriteArrayList

class QuotesUpdateHandler(private val notificationQueue: BlockingQueue<QuotesUpdate>): Thread(QuotesUpdateHandler::class.java.name) {

    companion object {
        val LOGGER = Logger.getLogger(QuotesUpdateHandler::class.java.name)
    }
    private val connections = CopyOnWriteArrayList<ClientHandler>()

    override fun run() {
        while (true) {
            val notification = notificationQueue.take()
            if (connections.size > 0) {
                val protoNotification = ProtocolMessages.QuotesUpdate
                        .newBuilder()
                        .setAssetId(notification.asset)
                        .setPrice(notification.price.toDouble())
                        .setVolume(notification.volume.toDouble())
                        .build()
                val disconnected = LinkedList<ClientHandler>()
                connections.forEach {
                    try {
                        it.writeOutput(ByteHelper.toByteArray(MessageType.QUOTES_UPDATE_NOTIFICATION.type, protoNotification.serializedSize, protoNotification.toByteArray()))
                    } catch (exception: Exception){
                        disconnected.add(it)
                        LOGGER.info("Removed quotes notification subscription from ${it.clientHostName}")
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
        LOGGER.info("Added quotes notification subscription from ${handler.clientHostName}")
    }
}