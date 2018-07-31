package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class LoggerConfig {
    @Bean
    open fun singleLimitOrderContextPreprocessorLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger(SingleLimitOrderPreprocessor::class.java.name)
    }
}