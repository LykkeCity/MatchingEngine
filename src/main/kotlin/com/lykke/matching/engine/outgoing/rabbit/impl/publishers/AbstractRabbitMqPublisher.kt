package com.lykke.matching.engine.outgoing.rabbit.impl.publishers

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitRecoverEvent
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.apache.log4j.Logger
import org.springframework.context.ApplicationEventPublisher
import java.util.concurrent.BlockingQueue

abstract class AbstractRabbitMqPublisher<T>(private val uri: String,
                                            private val exchangeName: String,
                                            private val queueName: String,
                                            val queue: BlockingQueue<out T>,
                                            private val appName: String,
                                            private val appVersion: String,
                                            private val exchangeType: BuiltinExchangeType,
                                            private val LOGGER: ThrottlingLogger,
                                            private val MESSAGES_LOGGER: Logger,
                                            private val METRICS_LOGGER: MetricsLogger,
                                            private val STATS_LOGGER: Logger,
                                            private val applicationEventPublisher: ApplicationEventPublisher,

                                            /** null if do not need to log */
                                               private val messageDatabaseLogger: DatabaseLogger<T>? = null) : Thread() {

    companion object {
        private const val LOG_COUNT = 1000
        private const val CONNECTION_NAME_FORMAT = "[Pub] %s %s to %s"
        private const val RECONNECTION_INTERVAL = 1000L
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
            this.connection = factory.newConnection(CONNECTION_NAME_FORMAT.format(appName, appVersion, exchangeName))
            this.channel = connection!!.createChannel()
            channel!!.exchangeDeclare(exchangeName, exchangeType, true)

            LOGGER.info("Connected to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName")
            publishRecoverEvent()
            return true
        } catch (e: Exception) {
            publishFailureEvent(null)
            LOGGER.error("Unable to connect to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName: ${e.message}", e)
            return false
        }
    }

    protected abstract fun getRabbitPublishRequest(item: T): RabbitPublishRequest

    private fun publish(item: T) {
        var isLogged = false
        while (true) {
            try {
                val startTime = System.nanoTime()

                val rabbitPublishRequest = getRabbitPublishRequest(item)

                if (!isLogged) {
                    logMessage(item, rabbitPublishRequest.stringRepresentation)
                    isLogged = true
                }

                val startPersistTime = System.nanoTime()
                channel!!.basicPublish(exchangeName, rabbitPublishRequest.routingKey, rabbitPublishRequest.props, rabbitPublishRequest.body)

                val endPersistTime = System.nanoTime()
                val endTime = System.nanoTime()
                fixTime(startTime, endTime, startPersistTime, endPersistTime)

                return
            } catch (exception: Exception) {
                publishFailureEvent(item)
                LOGGER.error("Exception during RabbitMQ publishing: ${exception.message}", exception)
                METRICS_LOGGER.logError("Exception during RabbitMQ publishing: ${exception.message}", exception)
                tryConnectUntilSuccess()
            }
        }
    }

    private fun publishRecoverEvent() {
        val message = "Rabbit MQ publisher: $queueName recovered"
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
        applicationEventPublisher.publishEvent(RabbitRecoverEvent(queueName))
    }

    private fun publishFailureEvent(event: T?) {
        val message = "Rabbit MQ publisher: $queueName failed"
        LOGGER.error(message)
        METRICS_LOGGER.logError(message)
        applicationEventPublisher.publishEvent(RabbitFailureEvent(queueName, event))
    }

    private fun logMessage(item: T, stringRepresentation: String?) {
        if (messageDatabaseLogger != null && stringRepresentation != null) {
            MESSAGES_LOGGER.info("$exchangeName : $stringRepresentation")
            messageDatabaseLogger.log(item, stringRepresentation)
        }
    }

    override fun run() {
        tryConnectUntilSuccess()
        while (true) {
            val item = queue.take()
            publish(item)
        }
    }

    private fun tryConnectUntilSuccess() {
        while (!connect()) {
            Thread.sleep(RECONNECTION_INTERVAL)
        }
    }

    private fun fixTime(startTime: Long, endTime: Long, startPersistTime: Long, endPersistTime: Long) {
        messagesCount++
        totalPersistTime += (endPersistTime - startPersistTime).toDouble() / LOG_COUNT
        totalTime += (endTime - startTime).toDouble() / LOG_COUNT

        if (messagesCount % LOG_COUNT == 0L) {
            STATS_LOGGER.info("Exchange: $exchangeName. Messages: $LOG_COUNT. Total: ${PrintUtils.convertToString(totalTime)}. " +
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${NumberUtils.roundForPrint2(100 * totalPersistTime / totalTime)} %")
            totalPersistTime = 0.0
            totalTime = 0.0
        }
    }
}