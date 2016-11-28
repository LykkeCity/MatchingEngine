package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.outgoing.JsonSerializable
import org.apache.log4j.Logger
import java.io.IOException
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(val host: String, val port: Int, val username: String, val password: String, val exchangeName: String, val queue: BlockingQueue<JsonSerializable>) : Thread() {

    companion object {
        val LOGGER = Logger.getLogger(RabbitMqPublisher::class.java.name)
        val EXCHANGE_TYPE = "fanout"
    }

    var connection: Connection? = null
    var channel: Channel? = null

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
        while (true) {
            try {
                channel!!.basicPublish(exchangeName, "", null, item.toJson().toByteArray())
                return
            } catch (exception: IOException) {
                LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
                connect()
            }
        }
    }
}