package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.annotation.Scheduled
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

class QueueSizeHealthChecker(private val monitoredComponent: MonitoredComponent,
                             private val nameToInputQueue: Map<String, BlockingQueue<*>>,
                             private val queueMaxSize: Int,
                             private val queueRecoverSize: Int) {

    private companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(QueueSizeHealthChecker::class.java)!!

        const val QUEUE_REACHED_THRESHOLD_MESSAGE = "Queue: %s, has reached max size threshold, current queue size is %d"
        const val QUEUE_RECOVERED_MESSAGE = "Queue: %s, has normal size again, current queue size is %d"
    }

    private var longQueues = HashSet<String>()
    private var lastCheckLongQueuesSize = 0

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @PostConstruct
    fun init() {
        val monitoredQueueNames = nameToInputQueue.keys.joinToString()
        LOGGER.info("Starting health monitoring for queues: $monitoredQueueNames")
    }

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
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, monitoredComponent))
        } else {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, monitoredComponent))
        }
    }

    fun checkQueueRecovered(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size <= queueRecoverSize && longQueues.remove(nameToQueue.key)) {
            val logMessage = QUEUE_RECOVERED_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            METRICS_LOGGER.logWarning(logMessage)
            LOGGER.info(logMessage)
        }
    }

    fun checkQueueReachedMaxLimit(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size >= queueMaxSize && !longQueues.contains(nameToQueue.key)) {
            longQueues.add(nameToQueue.key)
            val logMessage = QUEUE_REACHED_THRESHOLD_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            METRICS_LOGGER.logError(logMessage)
            LOGGER.error(logMessage)
        }
    }
}