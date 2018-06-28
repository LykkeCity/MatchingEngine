package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.rabbit.events.TrustedLimitOrdersReportEvent
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct

@Component
class TrustedClientsLimitOrdersListener {
    private val queue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()

    @Autowired
    private lateinit var rabbitMqService: RabbitMqService

    @Autowired
    private lateinit var config: Config

    @EventListener
    fun process(trustedLimitOrdersReportEvent: TrustedLimitOrdersReportEvent) {
        queue.put(trustedLimitOrdersReportEvent.limitOrdersReport)
    }

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqService.startPublisher(config.me.rabbitMqConfigs.limitOrders, queue,
                config.me.name,
                AppVersion.VERSION,
                null)
    }
}