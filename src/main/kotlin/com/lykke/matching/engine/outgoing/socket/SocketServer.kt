package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.utils.config.AzureConfig
import org.apache.log4j.Logger
import java.net.ServerSocket
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class SocketServer(val config: AzureConfig, val connectionsHolder: ConnectionsHolder): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(SocketServer::class.java.name)
    }

    override fun run() {
        val maxConnections = config.me.serverOrderBookMaxConnections
        val clientHandlerThreadPool = Executors.newFixedThreadPool(maxConnections)

        val port = config.me.serverOrderBookPort
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                val connection = Connection(clientConnection, LinkedBlockingQueue<JsonSerializable>())
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