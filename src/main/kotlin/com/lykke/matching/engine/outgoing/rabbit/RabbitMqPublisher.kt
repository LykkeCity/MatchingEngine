package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(
        private val uri: String,
        private val exchangeName: String,
        private val queue: BlockingQueue<JsonSerializable>,
        private val logMessage: Boolean) : Thread() {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(RabbitMqPublisher::class.java.name)
        val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqPublisher::class.java.name}.message")
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val EXCHANGE_TYPE = "fanout"
    }

    var connection: Connection? = null
    var channel: Channel? = null

    private fun connect(): Boolean {
        val factory = ConnectionFactory()
        factory.setUri(uri)

        LOGGER.info("Connecting to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName")

        try {
            this.connection = factory.newConnection()
            this.channel = connection!!.createChannel()
            channel!!.exchangeDeclare(exchangeName, EXCHANGE_TYPE, true)

            LOGGER.info("Connected to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName")

            return true
        } catch (e: Exception) {
            LOGGER.error("Unable to connect to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName: ${e.message}", e)
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

    private fun publish(item: JsonSerializable) {
        var isLogged = false
        while (true) {
            try {
                val stringValue = item.toJson()
                if (logMessage && !isLogged) {
                    MESSAGES_LOGGER.info("$exchangeName : $stringValue")
                    isLogged = true
                }
                channel!!.basicPublish(exchangeName, "", null, stringValue.toByteArray())
                return
            } catch (exception: Exception) {
                LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
                METRICS_LOGGER.logError( "Exception during RabbitMQ publishing: ${exception.message}", exception)
                while (!connect()) {
                    Thread.sleep(1000)
                }
            }
        }
    }
}