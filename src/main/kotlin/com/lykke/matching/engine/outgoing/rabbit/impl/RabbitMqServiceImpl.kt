package com.lykke.matching.engine.outgoing.rabbit.impl

import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.rabbitmq.client.BuiltinExchangeType
import java.util.concurrent.BlockingQueue

class RabbitMqServiceImpl: RabbitMqService<Event<*>> {
    override fun startPublisher(config: RabbitConfig, queue: BlockingQueue<out Event<*>>, appName: String, appVersion: String, exchangeType: BuiltinExchangeType, messageDatabaseLogger: DatabaseLogger?) {


    }
}