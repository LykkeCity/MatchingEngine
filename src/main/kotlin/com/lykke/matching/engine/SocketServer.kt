package com.lykke.matching.engine

import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.messages.MessageWrapper
import org.apache.log4j.Logger
import java.net.ServerSocket
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.regex.Pattern

class SocketServer(val config: Properties): Runnable {

    companion object {
        val LOGGER = Logger.getLogger(SocketServer::class.java.name)
    }

    val DEFAULT_PORT = 8888
    val DEFAULT_MAX_CONNECTIONS = 10

    val messagesQueue: BlockingQueue<MessageWrapper> = LinkedBlockingQueue<MessageWrapper>()


    override fun run() {
        val maxConnections = config.getInt("server.max.connections") ?: DEFAULT_MAX_CONNECTIONS
        val clientHandlerThreadPool = Executors.newFixedThreadPool(maxConnections)

        val dbConfig = loadConfig(config)["Db"] as Map<String, String>
        val messageProcessor = MessageProcessor(config, dbConfig, messagesQueue)
        messageProcessor.start()

        val port = config.getInt("server.port") ?: DEFAULT_PORT
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                if (isConnectionAllowed(getWhiteList(), clientConnection.inetAddress.hostAddress)) {
                    clientHandlerThreadPool.submit(ClientHandler(messagesQueue, clientConnection))
                } else {
                    clientConnection.close()
                    LOGGER.info("Connection from host ${clientConnection.inetAddress.hostAddress} is not allowed.")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
        } finally {
            socket.close()
        }
    }

    fun isConnectionAllowed(whitelist: List<String>?, host: String): Boolean {
        if (whitelist != null) {
            whitelist.forEach {
                if (Pattern.compile(it).matcher(host).matches()) {
                    return true
                }
            }
            return false
        }
        return true
    }

    fun getWhiteList() : List<String>? {
        val whiteListStr = (loadConfig(config)["MatchingEngine"] as Map<String, Any>).get("WhiteList") as String?
        if (whiteListStr != null) {
            return whiteListStr.split(";")
        }
        return null
    }
}