package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.socket.SocketServer
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.utils.AppVersion
import com.lykke.utils.alivestatus.processor.AliveStatusProcessorFactory
import com.lykke.utils.logging.MetricsLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.env.get
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.SchedulingConfigurer
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.scheduling.config.ScheduledTaskRegistrar

@Configuration
@EnableScheduling
open class AppConfiguration: SchedulingConfigurer {
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
        threadPoolTaskScheduler.threadNamePrefix = "scheduled-task"
        threadPoolTaskScheduler.poolSize = environment["concurent.pool.size"].toInt()
        return threadPoolTaskScheduler
    }

    @Bean
    open fun socketServer(): Runnable {
        return SocketServer { appInitialData ->
            MetricsLogger.getLogger().logWarning("Spot.${config.me.name} ${AppVersion.VERSION} : " +
                    "Started : ${appInitialData.ordersCount} orders, ${appInitialData.stopOrdersCount} " +
                    "stop orders,${appInitialData.balancesCount} " +
                    "balances for ${appInitialData.clientsCount} clients")
        }
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

