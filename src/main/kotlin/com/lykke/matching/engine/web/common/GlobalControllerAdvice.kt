package com.lykke.matching.engine.web.common

import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class GlobalControllerAdvice {

    private companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(GlobalControllerAdvice::class.java)
    }

    @ExceptionHandler
    fun handleException(e: Exception) {
        val message = "Unhandled exception occurred in web component"
        METRICS_LOGGER.logError(message, e)
        LOGGER.error(message, e)
    }
}