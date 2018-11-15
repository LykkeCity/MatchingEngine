package com.lykke.matching.engine.outgoing.rabbit.impl.publishers

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.MessageProperties
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.springframework.context.ApplicationEventPublisher
import java.util.concurrent.BlockingQueue

@Deprecated("Deprecated rabbit publisher, please consider to use new format")
class RabbitMqOldFormatPublisher(uri: String,
                                 exchangeName: String,
                                 publisherName: String,
                                 queue: BlockingQueue<out Any>,
                                 appName: String,
                                 appVersion: String,
                                 exchangeType: BuiltinExchangeType,
                                 private val gson: Gson,
                                 applicationEventPublisher: ApplicationEventPublisher,
                                 messageDatabaseLogger: DatabaseLogger<Any>? = null) : AbstractRabbitMqPublisher<Any>(uri, exchangeName, publisherName,
        queue, appName, appVersion, exchangeType, LOGGER,
        MESSAGES_LOGGER, METRICS_LOGGER, STATS_LOGGER, applicationEventPublisher, messageDatabaseLogger) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RabbitMqOldFormatPublisher::class.java.name)
        private val MESSAGES_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.message")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val STATS_LOGGER = Logger.getLogger("${RabbitMqOldFormatPublisher::class.java.name}.stats")
    }

    override fun getRabbitPublishRequest(item: Any): RabbitPublishRequest {
        val jsonString = gson.toJson(item)
        val body = jsonString.toByteArray()

        return RabbitPublishRequest(StringUtils.EMPTY, body, jsonString, MessageProperties.MINIMAL_PERSISTENT_BASIC)
    }
}