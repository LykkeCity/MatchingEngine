package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.config.spring.InputQueue
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class InputQueueSizeHealthChecker {

    private companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(InputQueueSizeHealthChecker::class.java)!!

        const val QUEUE_REACHED_THRESHOLD_MESSAGE = "Queue: %s, has reached max size threshold, current queue size is %d"
        const val QUEUE_RECOVERED_MESSAGE = "Queue: %s, has normal size again, current queue size is %d"
    }

    private var longQueues = HashSet<String>()
    private var lastCheckLongQueuesSize = 0

    @Autowired
    @InputQueue
    private lateinit var nameToInputQueue: Map<String, BlockingQueue<*>>

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @Autowired
    private lateinit var config: Config

    @Scheduled(fixedRateString = "#{Config.me.queueConfig.queueSizeHealthCheckInterval}", initialDelayString = "#{Config.me.queueConfig.queueSizeHealthCheckInterval}")
    fun checkQueueSize() {
        nameToInputQueue.forEach {
            checkQueueReachedMaxLimit(it)
            checkQueueRecovered(it)
        }

        sendHealthEventIfNeeded()
        lastCheckLongQueuesSize = longQueues.size
    }

    fun sendHealthEventIfNeeded() {
        if (lastCheckLongQueuesSize == longQueues.size) {
            return
        }

        if (longQueues.isNotEmpty()) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.INPUT_QUEUE))
        } else {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.INPUT_QUEUE))
        }
    }

    fun checkQueueRecovered(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size <= config.me.queueConfig.recoverQueueSizeLimit && longQueues.remove(nameToQueue.key)) {
            val logMessage = QUEUE_RECOVERED_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            METRICS_LOGGER.logWarning(logMessage)
            LOGGER.info(logMessage)
        }
    }

    fun checkQueueReachedMaxLimit(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size >= config.me.queueConfig.maxQueueSizeLimit && !longQueues.contains(nameToQueue.key)) {
            longQueues.add(nameToQueue.key)
            val logMessage = QUEUE_REACHED_THRESHOLD_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            METRICS_LOGGER.logError(logMessage)
            LOGGER.error(logMessage)
        }
    }
}