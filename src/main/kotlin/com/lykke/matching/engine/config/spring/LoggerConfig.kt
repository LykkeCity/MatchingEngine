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
    open fun singleLimitOrderContextPreprocessorLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger(SingleLimitOrderPreprocessor::class.java.name)
    }

    @PostConstruct
    open fun init() {
        AppInitializer.init()
        MetricsLogger.init("ME", config.slackNotifications)
        ThrottlingLogger.init(config.throttlingLogger)
    }
}