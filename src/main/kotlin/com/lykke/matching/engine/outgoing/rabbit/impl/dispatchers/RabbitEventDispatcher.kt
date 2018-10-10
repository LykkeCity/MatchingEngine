package com.lykke.matching.engine.outgoing.rabbit.impl.dispatchers

import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitRecoverEvent
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.PostConstruct

class RabbitEventDispatcher<E>(private val dispatcherName: String,
                               private val inputDeque: BlockingDeque<E>,
                               private val queueNameToQueue: Map<String, BlockingQueue<E>>) : Thread(dispatcherName) {

    companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(RabbitEventDispatcher::class.java)
    }

    private var failedEventConsumers = HashSet<String>()

    private var maintenanceModeLock = ReentrantLock()
    private var maintenanceModeCondition = maintenanceModeLock.newCondition()

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    override fun run() {
        try {
            while (true) {
                val event = inputDeque.take()
                dispatchEventToEventListeners(event)
            }
        } catch (e: Exception) {
            logException("Error occurred in events dispatcher thread for dispatcher: $dispatcherName", e)
        }
    }

    @PostConstruct
    private fun init() {
        this.start()
    }

    private fun dispatchEventToEventListeners(event: E) {
        try {
            maintenanceModeLock.lock()

            while (failedEventConsumers.size == queueNameToQueue.size) {
                maintenanceModeCondition.await()
            }

            queueNameToQueue.keys.forEach {
                dispatchEventToEventListener(event, it)
            }
        } finally {
            maintenanceModeLock.unlock()
        }
    }

    private fun dispatchEventToEventListener(event: E, listenerName: String) {
        try {
            if (failedEventConsumers.contains(listenerName)) {
                return
            }
            queueNameToQueue[listenerName]?.put(event)
        } catch (e: Exception) {
            //normally never occur, exist to be sure event is dispatched to the rest of listeners
            logException("Failed to dispatch event, in dispatcher: $dispatcherName, for listener: $listenerName", e)
        }
    }

    @EventListener
    private fun onRabbitFailure(rabbitFailureEvent: RabbitFailureEvent<E>) {
        if (!queueNameToQueue.keys.contains(rabbitFailureEvent.publisherName)) {
            return
        }

        try {
            maintenanceModeLock.lock()
            failedEventConsumers.add(rabbitFailureEvent.publisherName)

            logError("Rabbit MQ publisher ${rabbitFailureEvent.publisherName} crashed, count of functional publishers is ${queueNameToQueue.size - failedEventConsumers.size}")

            val failedConsumerQueue = queueNameToQueue[rabbitFailureEvent.publisherName]

            failedConsumerQueue?.reversed()?.forEach {
                inputDeque.putFirst(it)
            }

            rabbitFailureEvent.failedEvent?.let { inputDeque.putFirst(it) }

            if (queueNameToQueue.size == failedEventConsumers.size) {
                logError("All Rabbit MQ publishers crashed, dispatcher: $dispatcherName")
                applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.RABBIT, dispatcherName))
            }

            failedConsumerQueue?.clear()
        } catch (e: Exception) {
            logException("Error occurred on dispatcher failure recording in dispatcher: $dispatcherName", e)
        } finally {
            maintenanceModeLock.unlock()
        }
    }

    @EventListener
    private fun onRabbitRecover(rabbitRecoverEvent: RabbitRecoverEvent) {
        if (!queueNameToQueue.keys.contains(rabbitRecoverEvent.publisherName)) {
            return
        }

        try {
            maintenanceModeLock.lock()
            log("Rabbit MQ publisher recovered: ${rabbitRecoverEvent.publisherName}, count of functional publishers is ${queueNameToQueue.size - failedEventConsumers.size}")
            failedEventConsumers.remove(rabbitRecoverEvent.publisherName)
            maintenanceModeCondition.signal()

            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT, dispatcherName))
        } catch (e: Exception) {
            logException("Error occurred on rabbit dispatcher recovery from maintenance mode for exchange: $dispatcherName", e)
        } finally {
            maintenanceModeLock.unlock()
        }
    }

    private fun logException(message: String, e: Exception) {
        METRICS_LOGGER.logError(message, e)
        LOGGER.error(message, e)
    }

    private fun logError(message: String) {
        METRICS_LOGGER.logError(message)
        LOGGER.error(message)
    }

    private fun log(message: String){
        METRICS_LOGGER.logWarning(message)
        LOGGER.info(message)
    }
}