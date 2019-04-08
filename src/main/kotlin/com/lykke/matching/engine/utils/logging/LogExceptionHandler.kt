package com.lykke.matching.engine.utils.logging

import com.lmax.disruptor.ExceptionHandler
import com.lykke.utils.logging.MetricsLogger

class LogExceptionHandler: ExceptionHandler<Any> {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun handleOnStartException(ex: Throwable?) {
        METRICS_LOGGER.logError("Exception occurred while starting log system ${ex?.let {": " + ex.message}}")
        ex?.printStackTrace()
    }

    override fun handleEventException(ex: Throwable?, sequence: Long, event: Any?) {
        METRICS_LOGGER.logError("Exception occurred while performing log operation ${ex?.let {": " + ex.message}}")
        ex?.printStackTrace()
    }

    override fun handleOnShutdownException(ex: Throwable?) {
        METRICS_LOGGER.logError("Exception occurred while stopping log system ${ex?.let {": " + ex.message}}")
        ex?.printStackTrace()
    }
}