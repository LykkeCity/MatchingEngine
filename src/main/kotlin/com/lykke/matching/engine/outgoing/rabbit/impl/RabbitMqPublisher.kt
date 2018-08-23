package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.logging.LogMessageTransformer
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqPublisher(uri: String,
                        exchangeName: String,
                        queue: BlockingQueue<out Event<*>>,
                        appName: String,
                        appVersion: String,
                        exchangeType: BuiltinExchangeType,
                        private val jsonMessageTransformer: LogMessageTransformer,
                        /** null if do not need to log */
                        messageDatabaseLogger: DatabaseLogger? = null) : AbstractRabbitMqPublisher<Event<*>>(uri, exchangeName,
        queue, appName, appVersion, exchangeType, LOGGER,
        MESSAGES_LOGGER, METRICS_LOGGER, STATS_LOGGER, messageDatabaseLogger) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RabbitMqOldFormatPublisher::class.java.name)
        private val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.message")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val STATS_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.stats")
    }

    override fun getRoutingKey(item: Event<*>): String {
        return item.header.messageType.id.toString()
    }

    override fun getBody(item: Event<*>): ByteArray {
        return item.buildGeneratedMessage().toByteArray()
    }

    override fun getProps(item: Event<*>): AMQP.BasicProperties {
        val headers = mapOf(Pair("MessageType", item.header.messageType.id),
                Pair("SequenceNumber", item.header.sequenceNumber),
                Pair("MessageId", item.header.messageId),
                Pair("RequestId", item.header.requestId),
                Pair("Version", item.header.version),
                Pair("Timestamp", item.header.timestamp.time),
                Pair("EventType", item.header.eventType))

        // MINIMAL_PERSISTENT_BASIC + headers
        return AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .headers(headers)
                .build()
    }

    override fun getLogMessage(item: Event<*>): Message {
        return jsonMessageTransformer.transform(item)
    }
}