package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.env.get
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.SchedulingConfigurer
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.scheduling.config.ScheduledTaskRegistrar
import java.util.concurrent.RejectedExecutionException

@Configuration
@EnableScheduling
open class AppConfiguration : SchedulingConfigurer {
    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var environment: Environment

    override fun configureTasks(taskRegistrar: ScheduledTaskRegistrar) {
        taskRegistrar.setTaskScheduler(taskScheduler())
    }

    @Bean
    open fun taskScheduler(): TaskScheduler {
        val threadPoolTaskScheduler = ThreadPoolTaskScheduler()
        threadPoolTaskScheduler.threadNamePrefix = "scheduled-task-"
        threadPoolTaskScheduler.poolSize = environment["concurent.pool.size"].toInt()
        return threadPoolTaskScheduler
    }

    @Bean
    open fun clientRequestThreadPool(@Value("\${concurrent.client.request.pool.core.pool.size}") corePoolSize: Int,
                                     @Value("#{Config.me.socket.maxConnections}") maxPoolSize: Int): ThreadPoolTaskExecutor {
        val threadPoolTaskExecutor = ThreadPoolTaskExecutor()
        threadPoolTaskExecutor.threadNamePrefix = "client-request-"
        threadPoolTaskExecutor.setQueueCapacity(0)
        threadPoolTaskExecutor.corePoolSize = corePoolSize
        threadPoolTaskExecutor.maxPoolSize = maxPoolSize

        threadPoolTaskExecutor.setRejectedExecutionHandler { rejectedTask, executor ->
            val message = "Task rejected from client handler thread pool, client can not be connected to ME, " +
                    "rejected tasks: [$rejectedTask] " +
                    "active threads size ${executor.activeCount}, " +
                    "max pool size ${executor.maximumPoolSize}"

            MetricsLogger.getLogger().logError(message)
            Logger.getLogger("ClientThreadPoolRejectionHandler").error(message)

            throw RejectedExecutionException()
        }
        return threadPoolTaskExecutor
    }

    @Bean
    open fun orderBookSubscribersThreadPool(@Value("\${concurrent.orderbook.subscribers.pool.core.pool.size}") corePoolSize: Int,
                                            @Value("#{Config.me.serverOrderBookMaxConnections}") maxPoolSize: Int?): ThreadPoolTaskExecutor? {
        if (config.me.serverOrderBookPort == null) {
            return null
        }

        val threadPoolTaskExecutor = ThreadPoolTaskExecutor()
        threadPoolTaskExecutor.threadNamePrefix = "orderbook-subscriber-"
        threadPoolTaskExecutor.setQueueCapacity(0)
        threadPoolTaskExecutor.corePoolSize = corePoolSize
        threadPoolTaskExecutor.maxPoolSize = maxPoolSize!!

        threadPoolTaskExecutor.setRejectedExecutionHandler { rejectedTask, executor ->
            val message = "Task rejected from order book subscribe thread pool, " +
                    "rejected task: [$rejectedTask] " +
                    "active threads size ${executor.activeCount}, " +
                    "max pool size ${executor.maximumPoolSize}"

            MetricsLogger.getLogger().logError(message)
            Logger.getLogger("OrderBookSubscribersThreadPoolRejectionHandler").error(message)

            throw RejectedExecutionException()
        }

        return threadPoolTaskExecutor
    }

    @Bean
    open fun azureStatusProcessor(): Runnable {
        return AliveStatusProcessorFactory
                .createAzureProcessor(connectionString = config.me.db.matchingEngineConnString,
                        appName = config.me.name,
                        config = config.me.aliveStatus)
    }

    @Bean
    open fun monitoringStatsCollector(): MonitoringStatsCollector {
        return MonitoringStatsCollector()
    }
}