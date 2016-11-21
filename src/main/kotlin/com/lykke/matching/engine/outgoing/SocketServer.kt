package com.lykke.matching.engine.outgoing

import com.lykke.matching.engine.getInt
import org.apache.log4j.Logger
import java.net.ServerSocket
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class SocketServer(val config: Properties, val connectionsHolder: ConnectionsHolder): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(SocketServer::class.java.name)
    }

    val DEFAULT_PORT = 8889
    val DEFAULT_MAX_CONNECTIONS = 10

    override fun run() {
        val maxConnections = config.getInt("server.order.book.max.connections") ?: DEFAULT_MAX_CONNECTIONS
        val clientHandlerThreadPool = Executors.newFixedThreadPool(maxConnections)

        val port = config.getInt("server.order.book.port") ?: DEFAULT_PORT
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                val connection = Connection(clientConnection, LinkedBlockingQueue<OrderBook>())
                clientHandlerThreadPool.submit(connection)
                connectionsHolder.addConnection(connection)
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
        } finally {
            socket.close()
        }
    }
}