package com.lykke.matching.engine.socket

import com.lykke.matching.engine.getInt
import com.lykke.matching.engine.loadConfig
import com.lykke.matching.engine.logging.IP
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_CONNECTIONS_COUNT
import com.lykke.matching.engine.logging.ME_CONNECTIONS_DETAILS
import com.lykke.matching.engine.logging.ME_CONNECTIONS_INCOMING
import com.lykke.matching.engine.logging.ME_CONNECTIONS_OUTGOING
import com.lykke.matching.engine.logging.ME_STATUS
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.MetricsLogger.Companion.DATE_TIME_FORMATTER
import com.lykke.matching.engine.logging.NOTE
import com.lykke.matching.engine.logging.STATUS
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.socket.ConnectionStatus.Blocked
import com.lykke.matching.engine.socket.ConnectionStatus.Connected
import com.lykke.matching.engine.socket.ConnectionStatus.Disconnected
import org.apache.log4j.Logger
import java.net.ServerSocket
import java.time.LocalDateTime
import java.util.HashSet
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.regex.Pattern
import kotlin.concurrent.fixedRateTimer

class SocketServer(val config: Properties): Runnable {

    companion object {
        val LOGGER = Logger.getLogger(SocketServer::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val DEFAULT_PORT = 8888
    val DEFAULT_MAX_CONNECTIONS = 10

    val messagesQueue: BlockingQueue<MessageWrapper> = LinkedBlockingQueue<MessageWrapper>()
    val connections = HashSet<ClientHandler>()

    override fun run() {
        val maxConnections = config.getInt("server.max.connections") ?: DEFAULT_MAX_CONNECTIONS
        val clientHandlerThreadPool = Executors.newFixedThreadPool(maxConnections)

        val messageProcessor = MessageProcessor(config, loadConfig(config), messagesQueue)
        messageProcessor.start()

        METRICS_LOGGER.log(KeyValue(ME_STATUS, "True"))
        initMetricLogger()

        val port = config.getInt("server.port") ?: DEFAULT_PORT
        val socket = ServerSocket(port)
        LOGGER.info("Waiting connection on port: $port.")
        try {

            while (true) {
                val clientConnection = socket.accept()
                if (isConnectionAllowed(getWhiteList(), clientConnection.inetAddress.hostAddress)) {
                    val handler = ClientHandler(messagesQueue, clientConnection, this)
                    clientHandlerThreadPool.submit(handler)
                    connect(handler)
                } else {
                    clientConnection.close()
                    LOGGER.info("Connection from host ${clientConnection.inetAddress.hostAddress} is not allowed.")
                    METRICS_LOGGER.log(getMetricLine(clientConnection.inetAddress.hostAddress, Blocked.name, "Blocked due to white list"))
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Got exception: ", exception)
            METRICS_LOGGER.logError(this.javaClass.name, "Fatal exception", exception)
        } finally {
            METRICS_LOGGER.log(KeyValue(ME_STATUS, "False"))
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
        val whiteListStr = (loadConfig(config)["MatchingEngine"] as Map<String, Any>)["WhiteList"] as String?
        if (whiteListStr != null) {
            return whiteListStr.split(";")
        }
        return null
    }

    fun getMetricLine(ip: String, status: String, note: String): Line {
        return Line(ME_CONNECTIONS_DETAILS, arrayOf(
                KeyValue(IP, ip),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(DATE_TIME_FORMATTER)),
                KeyValue(STATUS, status),
                KeyValue(NOTE, note)))
    }

    fun initMetricLogger() {
        fixedRateTimer(name = "ConnectionsStatusLogger", initialDelay = 60000, period = 60000) {
            connections.forEach {
                if (!it.isConnected()) {
                    it.disconnect()
                }
            }
            METRICS_LOGGER.log(KeyValue(ME_CONNECTIONS_COUNT, connections.size.toString()))
        }

        fixedRateTimer(name = "ConnectionsSizesLogger", initialDelay = 60000, period = 60000) {
            var incoming: Long = 0
            var outgoing: Long = 0
            connections.forEach {
                incoming += it.incomingSize
                outgoing += it.outgoingSize
                it.incomingSize = 0
                it.outgoingSize = 0
            }
            METRICS_LOGGER.log(KeyValue(ME_CONNECTIONS_INCOMING, incoming.toString()))
            METRICS_LOGGER.log(KeyValue(ME_CONNECTIONS_OUTGOING, outgoing.toString()))
        }
    }

    fun connect(handler: ClientHandler, note: String = "") {
        connections.add(handler)
        METRICS_LOGGER.log(KeyValue(ME_CONNECTIONS_COUNT, connections.size.toString()))
        METRICS_LOGGER.log(getMetricLine(handler.clientHostName, Connected.name, note))
    }

    fun disconnect(handler: ClientHandler, note: String = "") {
        connections.remove(handler)
        METRICS_LOGGER.log(KeyValue(ME_CONNECTIONS_COUNT, connections.size.toString()))
        METRICS_LOGGER.log(getMetricLine(handler.clientHostName, Disconnected.name, note))
    }
}