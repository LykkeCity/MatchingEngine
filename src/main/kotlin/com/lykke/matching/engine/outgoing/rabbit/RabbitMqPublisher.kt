package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(
        private val uri: String,
        private val exchangeName: String,
        private val queue: BlockingQueue<JsonSerializable>,
        /** null if do not need to log */
        private val messageDatabaseLogger: MessageDatabaseLogger? = null) : Thread() {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RabbitMqPublisher::class.java.name)
        private val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqPublisher::class.java.name}.message")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val STATS_LOGGER = Logger.getLogger("${RabbitMqPublisher::class.java.name}.stats")
        private const val EXCHANGE_TYPE = "fanout"
        private const val LOG_COUNT = 1000
    }

    private var connection: Connection? = null
    private var channel: Channel? = null
    private var messagesCount: Long = 0
    private var totalPersistTime: Double = 0.0
    private var totalTime: Double = 0.0

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
                val startTime = System.nanoTime()
                val stringValue = item.toJson()
                messageDatabaseLogger?.let {
                    if (!isLogged) {
                        MESSAGES_LOGGER.info("$exchangeName : $stringValue")
                        it.log(item)
                        isLogged = true
                    }
                }
                val startPersistTime = System.nanoTime()
                channel!!.basicPublish(exchangeName, "", null, stringValue.toByteArray())
                val endPersistTime = System.nanoTime()
                val endTime = System.nanoTime()
                fixTime(startTime, endTime, startPersistTime, endPersistTime)
                return
            } catch (exception: Exception) {
                LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
                METRICS_LOGGER.logError("Exception during RabbitMQ publishing: ${exception.message}", exception)
                while (!connect()) {
                    Thread.sleep(1000)
                }
            }
        }
    }

    private fun fixTime(startTime: Long, endTime: Long, startPersistTime: Long, endPersistTime: Long) {
        messagesCount++
        totalPersistTime += (endPersistTime - startPersistTime).toDouble() / LOG_COUNT
        totalTime += (endTime - startTime).toDouble() / LOG_COUNT

        if (messagesCount % LOG_COUNT == 0L) {
            STATS_LOGGER.info("Exchange: $exchangeName. Messages: $LOG_COUNT. Total: ${PrintUtils.convertToString(totalTime)}. " +
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${RoundingUtils.roundForPrint2(100 * totalPersistTime / totalTime)} %")
            totalPersistTime = 0.0
            totalTime = 0.0
        }
    }
}