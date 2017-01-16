package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(val host: String, val port: Int, val username: String, val password: String, val exchangeName: String, val queue: BlockingQueue<JsonSerializable>) : Thread() {

    companion object {
        val LOGGER = Logger.getLogger(RabbitMqPublisher::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val EXCHANGE_TYPE = "fanout"
    }

    var connection: Connection? = null
    var channel: Channel? = null

    fun connect(): Boolean {
        LOGGER.info("Connecting to RabbitMQ: $host:$port, exchange: $exchangeName")

        try {
            val factory = ConnectionFactory()
            factory.host = host
            factory.port = port
            factory.username = username
            factory.password = password

            this.connection = factory.newConnection()
            this.channel = connection!!.createChannel()
            channel!!.exchangeDeclare(exchangeName, "fanout", true)

            LOGGER.info("Connected to RabbitMQ: $host:$port, exchange: $exchangeName")

            return true
        } catch (e: Exception) {
            LOGGER.error("Unable to connect to RabbitMQ: $host:$port, exchange: $exchangeName: ${e.message}", e)
            return false
        }
    }

    override fun run() {
        while (!connect()) {}
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
            } catch (exception: Exception) {
                LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
                METRICS_LOGGER.logError(this.javaClass.name, "Exception during RabbitMQ publishing: ${exception.message}", exception)
                connect()
            }
        }
    }
}