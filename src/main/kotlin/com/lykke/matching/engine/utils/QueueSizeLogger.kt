package com.lykke.matching.engine.utils

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.context.annotation.Profile
import java.util.concurrent.BlockingQueue
import java.util.stream.Collectors
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer

@Component
@Profile("default", "!local_config")
class QueueSizeLogger @Autowired constructor(val queues: Map<String, BlockingQueue<*>>,
                                             val config: Config) {
    companion object {
        val LOGGER = Logger.getLogger(QueueSizeLogger::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val ENTRY_SIZE_FORMAT = "%s: %d;"
        val ENTRY_SIZE_LIMIT_FORMAT = "%s queue is higher than limit"
        val ENTRY_DELIMITER = ". "
    }

    @PostConstruct
    fun start() {
        fixedRateTimer(name = "QueueSizeLogger", initialDelay = config.me.queueSizeLoggerInterval, period = config.me.queueSizeLoggerInterval) {
            log()
        }
    }

    private fun log() {
        val queueNameToQueueSize = getQueueNameToQueueSize(queues)

        logQueueSizes(queueNameToQueueSize)
        checkQueueSizeLimits(queueNameToQueueSize)
    }

    private fun logQueueSizes(nameToQueueSize: Map<String, Int>) {
        val logString = nameToQueueSize
                .entries
                .stream()
                .map({ entry -> ENTRY_SIZE_FORMAT.format(entry.key, entry.value) })
                .collect(Collectors.joining(ENTRY_DELIMITER))

        LOGGER.info(logString)
    }

    private fun checkQueueSizeLimits(nameToQueueSize: Map<String, Int>) {
        nameToQueueSize
                .forEach { entry ->
                    if (entry.value > config.me.queueSizeLimit) {
                        METRICS_LOGGER.logError(ENTRY_SIZE_LIMIT_FORMAT.format(entry.key))
                    }
                }
    }

    private fun getQueueNameToQueueSize(nameToQueue: Map<String, BlockingQueue<*>>): Map<String, Int> {
       return nameToQueue.mapValues { entry -> entry.value.size }
    }
}