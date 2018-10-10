package com.lykke.matching.engine.utils

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import java.util.stream.Collectors

@Component
@Profile("default", "!local_config")
class QueueSizeLogger @Autowired constructor(private val queues: Map<String, BlockingQueue<*>>,
                                             private val config: Config) {
    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        val ENTRY_FORMAT = "%s: %d;"
        val ENTRY_SIZE_LIMIT_FORMAT = "%s queue is higher than limit"
        val ENTRY_DELIMITER = ". "

        val LOG_THREAD_NAME = "QueueSizeLogger"
    }

    @Scheduled(fixedRateString = "#{Config.me.queueSizeLoggerInterval}",  initialDelayString = "#{Config.me.queueSizeLoggerInterval}")
    private fun log() {
        Thread.currentThread().name = LOG_THREAD_NAME
        val queueNameToQueueSize = getQueueNameToQueueSize(queues)

        logQueueSizes(queueNameToQueueSize)
        checkQueueSizeLimits(queueNameToQueueSize)
    }

    private fun logQueueSizes(nameToQueueSize: Map<String, Int>) {
        val logString = nameToQueueSize
                .entries
                .stream()
                .map({ entry -> ENTRY_FORMAT.format(entry.key, entry.value) })
                .collect(Collectors.joining(ENTRY_DELIMITER))

        LOGGER.info(logString)
    }

    private fun checkQueueSizeLimits(nameToQueueSize: Map<String, Int>) {
        nameToQueueSize
                .forEach { entry ->
                    if (entry.value > config.me.queueSizeLimit) {
                        val message = ENTRY_SIZE_LIMIT_FORMAT.format(entry.key)
                        METRICS_LOGGER.logError(message)
                        LOGGER.warn(message)
                    }
                }
    }

    private fun getQueueNameToQueueSize(nameToQueue: Map<String, BlockingQueue<*>>): Map<String, Int> {
       return nameToQueue.mapValues { entry -> entry.value.size }
    }
}