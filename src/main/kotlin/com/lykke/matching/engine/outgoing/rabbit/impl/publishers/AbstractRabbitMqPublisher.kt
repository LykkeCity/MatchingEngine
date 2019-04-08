package com.lykke.matching.engine.outgoing.rabbit.impl.publishers

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitReadyEvent
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.*
import org.slf4j.Logger
import org.springframework.context.ApplicationEventPublisher
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

abstract class AbstractRabbitMqPublisher<T>(private val uri: String,
                                            private val exchangeName: String,
                                            private val queueName: String,
                                            private val queue: BlockingQueue<out T>,
                                            private val appName: String,
                                            private val appVersion: String,
                                            private val exchangeType: BuiltinExchangeType,
                                            private val LOGGER: ThrottlingLogger,
                                            private val MESSAGES_LOGGER: Logger,
                                            private val METRICS_LOGGER: MetricsLogger,
                                            private val STATS_LOGGER: Logger,
                                            private val applicationEventPublisher: ApplicationEventPublisher,
                                            private val heartBeatTimeout: Long,
                                            private val handshakeTimeout: Long,
                                            /** null if do not need to log */
                                            private val messageDatabaseLogger: DatabaseLogger<T>? = null) : Runnable {

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


    @Volatile
    private var currentlyPublishedItem: T? = null

    private fun connect(): Boolean {
        val factory = ConnectionFactory()
        factory.setUri(uri)
        factory.requestedHeartbeat =  TimeUnit.MILLISECONDS.toSeconds(heartBeatTimeout).toInt()
        factory.handshakeTimeout =  handshakeTimeout.toInt()
        factory.isAutomaticRecoveryEnabled = false
        LOGGER.info("Connecting to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName")

        try {
            this.connection = factory.newConnection(CONNECTION_NAME_FORMAT.format(appName, appVersion, exchangeName))
            (this.connection as Connection).addBlockedListener(RmqBlockListener())

            this.channel = connection!!.createChannel()
            channel!!.exchangeDeclare(exchangeName, exchangeType, true)

            LOGGER.info("Connected to RabbitMQ: ${factory.host}:${factory.port}, exchange: $exchangeName")
            publishRabbitReadyEvent()
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
                val message = "Exception during RabbitMQ publishing (uri: $uri, exchange name: $exchangeName): ${exception.message}"
                LOGGER.error(message, exception)
                METRICS_LOGGER.logError(message, exception)
                tryConnectUntilSuccess()
            }
        }
    }

    private fun publishRabbitReadyEvent() {
        applicationEventPublisher.publishEvent(RabbitReadyEvent(queueName))
    }

    private fun publishFailureEvent(event: T?) {
        applicationEventPublisher.publishEvent(RabbitFailureEvent(queueName, event))
    }

    private fun logMessage(item: T, stringRepresentation: String?) {
        if (messageDatabaseLogger != null && stringRepresentation != null) {
            MESSAGES_LOGGER.info("$exchangeName : $stringRepresentation")
            messageDatabaseLogger.log(item, stringRepresentation)
        }
    }

    override fun run() {
        Thread.currentThread().name = "RabbitPublisher_$queueName"
        tryConnectUntilSuccess()
        while (true) {
            val item = queue.take()
            currentlyPublishedItem = item
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

    inner class RmqBlockListener : BlockedListener {
        override fun handleBlocked(reason: String?) {
            val message = "Rabbit mq publisher for queue $queueName received socket block signal from broker, reason: $reason"
            LOGGER.error(message)
            METRICS_LOGGER.logError(message)
            publishFailureEvent(currentlyPublishedItem)
        }

        override fun handleUnblocked() {
            publishRabbitReadyEvent()
        }
    }
}