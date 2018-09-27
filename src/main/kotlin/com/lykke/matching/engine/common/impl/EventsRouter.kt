package com.lykke.matching.engine.common.impl

import com.lykke.matching.engine.common.Listener
import com.lykke.matching.engine.services.CashInOutOperationService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import javax.annotation.PostConstruct

class EventsRouter<T>(private val queue: BlockingQueue<T>,
                      private val listeners: Optional<List<Listener<T>?>>) {

    private companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    @Autowired
    private lateinit var executor: Executor

    @PostConstruct
    fun processEvents() {
        executor.execute {
            try {
                while (true) {
                    val event = queue.take()
                    listeners.ifPresent { it.forEach { it?.onEvent(event) } }
                }
            } catch (e: InterruptedException) {
                LOGGER.warn("Event router thread is shutdown")
                Thread.currentThread().interrupt()
            }
        }
    }
}