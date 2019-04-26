package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

class InputQueueListener(private val inputQueue: BlockingQueue<MessageWrapper>,
                         private val preProcessor: MessagePreprocessor,
                         private val logger: ThrottlingLogger,
                         threadName: String) : Thread(threadName) {

    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val ERROR_MESSAGE = "Unable to pre process message"
    }

    @PostConstruct
    fun init() = start()

    @PreDestroy
    fun shutdown() {
        this.interrupt()
    }

    override fun run() {
        while (true) {
            try {
                if(!process()) {
                    return
                }
            } catch (e: Exception) {
                logger.error(ERROR_MESSAGE, e)
                METRICS_LOGGER.logError(ERROR_MESSAGE, e)
            }
        }
    }


    private fun process(): Boolean {
        try {
            val messageWrapper = inputQueue.take()
            preProcessor.preProcess(messageWrapper)
        } catch(e: InterruptedException) {
            this.interrupt()
            return false
        }

        return true
    }
}