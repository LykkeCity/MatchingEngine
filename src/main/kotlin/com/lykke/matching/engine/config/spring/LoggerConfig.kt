package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppInitializer
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
open class LoggerConfig {

    @Autowired
    private lateinit var config: Config

    @Bean(destroyMethod = "")
    open fun appStarterLogger(): Logger {
        return Logger.getLogger("AppStarter")
    }

    @Bean
    open fun multiLimitOrderPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("MultiLimitOrderPreProcessing")
    }

    @Bean
    open fun singleLimitOrderPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("SingleLimitOrderPreProcessing")
    }

    @Bean
    open fun cashInOutPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashInOutPreProcessing")
    }

    @Bean
    open fun cashTransferPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashTransferPreProcessing")
    }

    @Bean
    open fun limitOrderCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderCancelPreProcessing")
    }

    @Bean
    open fun limitOrderMassCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderMassCancelPreProcessing")
    }

    @PostConstruct
    open fun init() {
        AppInitializer.init()
        MetricsLogger.init("ME", config.slackNotifications)
        ThrottlingLogger.init(config.throttlingLogger)
    }
}