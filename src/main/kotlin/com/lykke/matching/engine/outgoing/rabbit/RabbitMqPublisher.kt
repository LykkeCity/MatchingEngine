package com.lykke.matching.engine.outgoing.rabbit

import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.logging.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(
        private val uri: String,
        private val exchangeName: String,
        private val queue: BlockingQueue<out OutgoingMessage>,
        private val appName: String,
        private val appVersion: String,
        private val exchangeType: BuiltinExchangeType,
        /** null if do not need to log */
        private val messageDatabaseLogger: MessageDatabaseLogger? = null) : Thread(RabbitMqPublisher::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RabbitMqPublisher::class.java.name)
        private val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqPublisher::class.java.name}.message")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val STATS_LOGGER = Logger.getLogger("${RabbitMqPublisher::class.java.name}.stats")
        private const val LOG_COUNT = 1000
        private const val CONNECTION_NAME_FORMAT = "[Pub] %s %s to %s"
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

    private fun publish(item: OutgoingMessage) {
        var isLogged = false
        while (true) {
            try {
                val startTime = System.nanoTime()
                var stringValue: String? = null
                val byteArrayValue: ByteArray
                val routingKey: String
                val props: AMQP.BasicProperties
                if (!item.isNewMessageFormat()) {
                    item as JsonSerializable
                    stringValue = item.toJson()
                    byteArrayValue = stringValue.toByteArray()
                    routingKey = ""
                    props = MessageProperties.MINIMAL_PERSISTENT_BASIC
                } else {
                    item as Event<*>
                    if (messageDatabaseLogger != null) {
                        stringValue = item.toString()
                    }
                    byteArrayValue = item.buildGeneratedMessage().toByteArray()
                    routingKey = item.header.messageType.id.toString()
                    val headers = mapOf(Pair("MessageType", item.header.messageType.id),
                            Pair("SequenceNumber", item.header.sequenceNumber),
                            Pair("MessageId", item.header.messageId),
                            Pair("RequestId", item.header.requestId),
                            Pair("Version", item.header.version),
                            Pair("Timestamp", item.header.timestamp.time),
                            Pair("EventType", item.header.eventType))

                    // MINIMAL_PERSISTENT_BASIC + headers
                    props = AMQP.BasicProperties.Builder()
                            .deliveryMode(2)
                            .headers(headers)
                            .build()
                }
                messageDatabaseLogger?.let {
                    if (!isLogged && stringValue != null) {
                        MESSAGES_LOGGER.info("$exchangeName : $stringValue")
                        it.log(MessageWrapper(item, stringValue))
                        isLogged = true
                    }
                }
                val startPersistTime = System.nanoTime()
                channel!!.basicPublish(exchangeName, routingKey, props, byteArrayValue)
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
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${NumberUtils.roundForPrint2(100 * totalPersistTime / totalTime)} %")
            totalPersistTime = 0.0
            totalTime = 0.0
        }
    }
}