package com.lykke.matching.engine.outgoing.rabbit.impl.dispatchers

import com.lykke.matching.engine.outgoing.rabbit.events.RabbitFailureEvent
import com.lykke.matching.engine.outgoing.rabbit.events.RabbitRecoverEvent
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.ReentrantLock

class RabbitEventDispatcher<E>(private val dispatcherName: String,
                               private val inputDeque: BlockingDeque<E>,
                               private val consumerNameToQueue: Map<String, BlockingQueue<E>>,
                               private val applicationEventPublisher: ApplicationEventPublisher) : Thread(dispatcherName) {

    companion object {
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOGGER = Logger.getLogger(RabbitEventDispatcher::class.java)
    }

    private var failedEventConsumers = HashSet<String>()

    private var maintenanceModeLock = ReentrantLock()
    private var maintenanceModeCondition = maintenanceModeLock.newCondition()

    override fun run() {
        try {
            while (true) {
                val event = inputDeque.take()
                dispatchEventToEventListeners(event)
            }
        } catch (e: Exception) {
            logException("Error occurred in events dispatcher thread for exchange: $dispatcherName", e)
        }
    }

    private fun dispatchEventToEventListeners(event: E) {
        try {
            maintenanceModeLock.lock()

            while (failedEventConsumers.size == consumerNameToQueue.size) {
                maintenanceModeCondition.await()
            }

            consumerNameToQueue.keys.forEach {
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
            consumerNameToQueue[listenerName]?.put(event)
        } catch (e: Exception) {
            //normally never occur, exist to be sure event is dispatched to the rest of listeners
            logException("Failed to dispatch event, for exchange: $dispatcherName, for listener: $listenerName", e)
        }
    }

    @EventListener
    private fun onRabbitFailure(rabbitFailureEvent: RabbitFailureEvent) {
        try {
            maintenanceModeLock.lock()
            failedEventConsumers.add(rabbitFailureEvent.publisherName)

            logError("Rabbit MQ publisher ${rabbitFailureEvent.publisherName} crashed, count of functional publishers is ${consumerNameToQueue.size - failedEventConsumers.size}")
            val blockingQueue = consumerNameToQueue[rabbitFailureEvent.publisherName]

            if (consumerNameToQueue.size == failedEventConsumers.size) {
                logError("All Rabbit MQ publishers crashed for exchange: $dispatcherName")
                blockingQueue?.forEach {
                    inputDeque.putFirst(it)
                }
            }

            blockingQueue?.clear()
        } catch (e: Exception) {
            logException("Error occurred on dispatcher failure recording for exchange: $dispatcherName", e)
        } finally {
            maintenanceModeLock.unlock()
        }
    }

    @EventListener
    private fun onRabbitRecover(rabbitRecoverEvent: RabbitRecoverEvent) {
        try {
            maintenanceModeLock.lock()
            failedEventConsumers.remove(rabbitRecoverEvent.publisherName)
            maintenanceModeCondition.signal()

            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.RABBIT))
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
}