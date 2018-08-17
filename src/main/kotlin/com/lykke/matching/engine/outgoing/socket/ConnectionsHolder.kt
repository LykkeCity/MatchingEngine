package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.ArrayList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CopyOnWriteArraySet
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer
import kotlin.concurrent.thread

@Component
class ConnectionsHolder {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(ConnectionsHolder::class.java.name)
    }

    private val connections = CopyOnWriteArraySet<Connection>()

    @Autowired
    private lateinit var  orderBookQueue: BlockingQueue<OrderBook>

    @PostConstruct
    fun initialize() {
        fixedRateTimer(name = "OrderBookActiveConnectionsCheck", initialDelay = 300000, period = 300000) {
            checkConnections()
        }

        thread(start = true, name = ConnectionsHolder::class.java.name) {
            while (true) {
                val orderBook = orderBookQueue.take()
                connections.forEach { connection -> connection.inputQueue.put(orderBook) }
            }
        }
    }

    fun addConnection(connection: Connection) {
        LOGGER.info("Adding new connection from ${connection.clientHostName}")
        connections.add(connection)
        connection.connectionHolder = this
    }

    fun removeConnection(connection: Connection) {
        connections.remove(connection)
    }

    fun checkConnections() {
        val activeConnections = ArrayList<String>(connections.size)
        connections.forEach {
            if (it.isClosed()) {
                LOGGER.error("Disconnected from ${it.clientHostName}")
                connections.remove(it)
            } else {
                activeConnections.add(it.clientHostName)
            }
        }

        LOGGER.info("${connections.size} active order book subscriber connections: $activeConnections")
    }
}