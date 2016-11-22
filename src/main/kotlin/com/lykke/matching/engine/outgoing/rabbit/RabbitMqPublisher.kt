package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.outgoing.JsonSerializable
import org.apache.log4j.Logger
import java.io.IOException
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher : Thread {

    companion object {
        val LOGGER = Logger.getLogger(RabbitMqPublisher::class.java.name)
        val EXCHANGE_TYPE = "fanout"
    }

    val queue: BlockingQueue<JsonSerializable>

    val host: String
    val port: Int
    val username: String
    val password: String

    val exchangeName: String

    var connection: Connection? = null
    var channel: Channel? = null

    constructor(host: String, port: Int, username: String, password: String, exchangeName: String, queue: BlockingQueue<JsonSerializable>) : super() {
        this.host = host
        this.port = port
        this.username = username
        this.password = password
        this.exchangeName = exchangeName
        this.queue = queue
    }

    fun connect() {
        val factory = ConnectionFactory()
        factory.host = host
        factory.port = port
        factory.username = username
        factory.password = password

        this.connection = factory.newConnection()
        this.channel = connection!!.createChannel()
    }

    override fun run() {
        connect()
        while (true) {
            val item = queue.take()
            publish(item)
        }
    }

    fun publish(item: JsonSerializable) {
        try {
            channel!!.basicPublish(exchangeName, "", null, item.toJson().toByteArray())
        } catch (exception: IOException) {
            LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
            //TODO reconnect
        }
    }
}