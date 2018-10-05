package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class TrustedClientsLimitOrdersListener {

    @Autowired
    private lateinit var trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>

    @Autowired
    private lateinit var rabbitMqOldService: RabbitMqService<Any>

    @Autowired
    private lateinit var config: Config

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqOldService.startPublisher(config.me.rabbitMqConfigs.limitOrders,
                TrustedClientsLimitOrdersListener::class.java.simpleName,
                trustedClientsLimitOrdersQueue,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT, null)
    }
}