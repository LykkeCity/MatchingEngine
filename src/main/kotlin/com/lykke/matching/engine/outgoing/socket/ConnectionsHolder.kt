package com.lykke.matching.engine.outgoing.socket

import com.lykke.matching.engine.outgoing.OrderBook
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.concurrent.fixedRateTimer

class ConnectionsHolder(val orderBookQueue: BlockingQueue<OrderBook>) : Thread() {
    companion object {
        val LOGGER = Logger.getLogger(ConnectionsHolder::class.java.name)
    }

    val connections = CopyOnWriteArraySet<Connection>()

    override fun run() {
        init()
        while (true) {
            val orderBook = orderBookQueue.take()
            connections.forEach { connection -> connection.inputQueue.put(orderBook) }
        }
    }

    fun init() {
        fixedRateTimer(name = "OrderBookActiveConnectionsCheck", initialDelay = 300000, period = 300000) {
            checkConnections()
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