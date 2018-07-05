package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class RabbitOrderBookListener {
    @Autowired
    private lateinit var rabbitOrderBookQueue: BlockingQueue<OrderBook>

    @Autowired
    private lateinit var rabbitMqService: RabbitMqService

    @Autowired
    private lateinit var config: Config

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqService.startPublisher(config.me.rabbitMqConfigs.orderBooks, rabbitOrderBookQueue,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT,
                null)
    }

    fun getOrderBookQueueSize(): Int {
        return rabbitOrderBookQueue.size
    }
}