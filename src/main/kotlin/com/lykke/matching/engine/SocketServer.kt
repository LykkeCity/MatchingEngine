package com.lykke.matching.engine

import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.messages.MessageWrapper
import org.apache.log4j.Logger
import java.net.ServerSocket
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class SocketServer {

    companion object {
        val LOGGER = Logger.getLogger(SocketServer::class.java.name)
    }

    val DEFAULT_PORT = 8888
    val DEFAULT_MAX_CONNECTIONS = 10

    val messagesQueue: BlockingQueue<MessageWrapper> = LinkedBlockingQueue<MessageWrapper>()

    val config: Properties

    constructor(config: Properties) {
        this.config = config
    }


    fun run() {
        val maxConnections = config.getInt("server.max.connections") ?: DEFAULT_MAX_CONNECTIONS
        val clientHandlerThreadPool = Executors.newFixedThreadPool(maxConnections)

        val messageProcessor = MessageProcessor(config, messagesQueue)
        messageProcessor.start()

        val port = config.getInt("server.port") ?: DEFAULT_PORT
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                clientHandlerThreadPool.submit(ClientHandler(messagesQueue, clientConnection))
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
        } finally {
            socket.close()
        }
    }
}