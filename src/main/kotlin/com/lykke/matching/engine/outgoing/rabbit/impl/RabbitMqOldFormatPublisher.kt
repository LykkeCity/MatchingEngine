package com.lykke.matching.engine.outgoing.rabbit.impl

import com.google.gson.Gson
import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.logging.LogMessageTransformer
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.MessageProperties
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class RabbitMqOldFormatPublisher(uri: String,
                                 exchangeName: String,
                                 queue: BlockingQueue<out Any>,
                                 appName: String,
                                 appVersion: String,
                                 exchangeType: BuiltinExchangeType,
                                 private val gson: Gson,
                                 private val messageTransformer: LogMessageTransformer,
                                 /** null if do not need to log */
                                 messageDatabaseLogger: DatabaseLogger? = null) : AbstractRabbitMqPublisher<Any>(uri, exchangeName,
        queue, appName, appVersion, exchangeType, LOGGER,
        MESSAGES_LOGGER, METRICS_LOGGER, STATS_LOGGER, messageDatabaseLogger){

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RabbitMqOldFormatPublisher::class.java.name)
        private val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.message")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val STATS_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.stats")
    }

    override fun getRoutingKey(item: Any): String {
        return StringUtils.EMPTY
    }

    override fun getBody(item: Any): ByteArray {
        return gson.toJson(item).toByteArray()
    }

    override fun getProps(item: Any): AMQP.BasicProperties {
        return MessageProperties.MINIMAL_PERSISTENT_BASIC
    }

    override fun getLogMessage(item: Any): Message {
        return messageTransformer.transform(item)
    }
}