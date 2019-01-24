package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.impl.publishers.RabbitMqPublisher
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue

@Service("rabbitMqService")
@Profile("default")
class RabbitMqServiceImpl(private val gson: Gson,
                          private val meConfig: Config,
                          private val applicationEventPublisher: ApplicationEventPublisher,
                          @Qualifier("rabbitPublishersThreadPool")
                          private val rabbitPublishersThreadPool: TaskExecutor) : RabbitMqService<Event<*>> {
    override fun startPublisher(config: RabbitConfig, publisherName: String,
                                queue: BlockingQueue<out Event<*>>, appName: String,
                                appVersion: String, exchangeType: BuiltinExchangeType,
                                messageDatabaseLogger: DatabaseLogger<Event<*>>?) {
        rabbitPublishersThreadPool.execute(RabbitMqPublisher(config.uri, config.exchange, publisherName, queue, appName, appVersion, exchangeType,
                gson, applicationEventPublisher, meConfig.me.rabbitMqConfigs.hearBeatTimeout, meConfig.me.rabbitMqConfigs.handshakeTimeout, messageDatabaseLogger))
    }
}