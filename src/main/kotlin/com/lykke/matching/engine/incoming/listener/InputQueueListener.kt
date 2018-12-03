package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

abstract class InputQueueListener(private val queue: BlockingQueue<MessageWrapper>,
                                  private val preProcessor: MessagePreprocessor,
                                  private val logger: ThrottlingLogger) : Thread(InputQueueListener::class.java.name) {

    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @PostConstruct
    fun init() = start()

    override fun run() {
        while (true) {
            val messageWrapper = queue.take()
            try {
                messageWrapper.messagePreProcessorStartTimestamp = System.nanoTime()
                preProcessor.preProcess(messageWrapper)
                messageWrapper.messagePreProcessorEndTimestamp = System.nanoTime()
            } catch (exception: Exception) {
                handlePreprocessingException(exception, messageWrapper)
            }
        }
    }

    private fun handlePreprocessingException(exception: Exception, message: MessageWrapper) {
        try {
            val context = message.context
            logger.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message} " +
                    if (context != null) "Error details: $context" else "", exception)

            METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
            preProcessor.writeResponse(message, MessageStatus.RUNTIME)
        } catch (e: Exception) {
            val errorMessage = "Got error during message preprocessing failure handling"
            e.addSuppressed(exception)
            logger.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }

}