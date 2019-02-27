package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.matching.engine.utils.monitoring.QueueSizeHealthChecker
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue

@Configuration
open class AppConfiguration {

    @Autowired
    private lateinit var config: Config


    @Bean
    open fun appInitData(genericLimitOrderService: GenericLimitOrderService,
                         genericStopLimitOrderService: GenericStopLimitOrderService,
                         balanceHolder: BalancesHolder): AppInitialData {
        return AppInitialData(genericLimitOrderService.initialOrdersCount, genericStopLimitOrderService.initialStopOrdersCount,
                balanceHolder.initialBalancesCount, balanceHolder.initialClientsCount)
    }

    @Bean
    open fun azureStatusProcessor(): Runnable {
        return AliveStatusProcessorFactory
                .createAzureProcessor(connectionString = config.me.db.matchingEngineConnString,
                        appName = config.me.name,
                        config = config.me.aliveStatus)
    }

    @Bean
    open fun inputQueueSizeChecker(@InputQueue namesToInputQueues: Map<String, BlockingQueue<*>>): QueueSizeHealthChecker {
        return QueueSizeHealthChecker(
                MonitoredComponent.INPUT_QUEUE,
                namesToInputQueues,
                config.me.queueConfig.maxQueueSizeLimit,
                config.me.queueConfig.recoverQueueSizeLimit)
    }

    @Bean
    open fun rabbitQueueSizeChecker(@RabbitQueue namesToInputQueues: Map<String, BlockingQueue<*>>): QueueSizeHealthChecker {
        return QueueSizeHealthChecker(
                MonitoredComponent.RABBIT_QUEUE,
                namesToInputQueues,
                config.me.queueConfig.rabbitMaxQueueSizeLimit,
                config.me.queueConfig.rabbitRecoverQueueSizeLimit)
    }

    @Bean
    open fun outgoingEventSendersByEventClass(specializedEventSenders: List<SpecializedEventSender>): Map<Class<*>, List<SpecializedEventSender>> {
        return specializedEventSenders.groupBy { it.getEventClass() }
    }

    @Bean
    open fun monitoringStatsCollector(): MonitoringStatsCollector {
        return MonitoringStatsCollector()
    }
}

