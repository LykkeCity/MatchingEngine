package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.impl.publishers.RabbitMqOldFormatPublisher
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service("rabbitMqOldService")
@Profile("default")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceImpl(private val gson: Gson,
                             @Value("#{Config.me.rabbitMqConfigs.heartBeatTimeout}")
                             private val heartBeatTimeout: Long,
                             @Value("#{Config.me.rabbitMqConfigs.handshakeTimeout}")
                             private val handshakeTimeout: Long,
                             private val applicationEventPublisher: ApplicationEventPublisher,
                             @Qualifier("rabbitPublishersThreadPool")
                             private val rabbitPublishersThreadPool: TaskExecutor) : RabbitMqService<Any> {
    override fun startPublisher(config: RabbitConfig,
                                publisherName: String,
                                queue: BlockingQueue<out Any>,
                                appName: String,
                                appVersion: String,
                                exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<Any>?) {
        rabbitPublishersThreadPool.execute(RabbitMqOldFormatPublisher(config.uri, config.exchange, publisherName, queue, appName, appVersion, exchangeType,
                gson, applicationEventPublisher, heartBeatTimeout, handshakeTimeout, messageDatabaseLogger))
    }
}