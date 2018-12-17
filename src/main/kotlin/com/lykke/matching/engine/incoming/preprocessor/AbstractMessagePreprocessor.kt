package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.ParsedData
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

abstract class AbstractMessagePreprocessor<T : ParsedData>(private val contextParser: ContextParser<T>,
                                                           private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                                           private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                                           private val logger: ThrottlingLogger) : MessagePreprocessor {

    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        try {
            parseAndPreProcess(messageWrapper)
        } catch (e: Exception) {
            handlePreProcessingException(e, messageWrapper)
        }
    }

    private fun parseAndPreProcess(messageWrapper: MessageWrapper) {
        messageWrapper.messagePreProcessorStartTimestamp = System.nanoTime()
        val parsedData = parse(messageWrapper)
        val parsedMessageWrapper = parsedData.messageWrapper
        val preProcessSuccess = when {
            !messageProcessingStatusHolder.isMessageProcessingEnabled() -> {
                writeResponse(parsedMessageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED, "Message processing is disabled")
                false
            }
            !messageProcessingStatusHolder.isHealthStatusOk() -> {
                val errorMessage = "Message processing is disabled"
                writeResponse(parsedMessageWrapper, MessageStatus.RUNTIME, errorMessage)
                logger.error(errorMessage)
                METRICS_LOGGER.logError(errorMessage)
                false
            }
            else -> preProcessParsedData(parsedData)
        }
        parsedMessageWrapper.messagePreProcessorEndTimestamp = System.nanoTime()
        if (preProcessSuccess) {
            preProcessedMessageQueue.put(parsedMessageWrapper)
        }
    }

    protected abstract fun preProcessParsedData(parsedData: T): Boolean

    protected fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String? = null) {
        val responseBuilder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)
        message?.let { responseBuilder.setStatusReason(it) }
        messageWrapper.writeNewResponse(responseBuilder)
    }

    private fun parse(messageWrapper: MessageWrapper): T {
        return contextParser.parse(messageWrapper)
    }

    private fun handlePreProcessingException(exception: Exception, message: MessageWrapper) {
        try {
            val context = message.context
            val errorMessage = "[${message.sourceIp}]: Got error during message preprocessing"
            logger.error("$errorMessage: ${exception.message} " +
                    if (context != null) "Error details: $context" else "", exception)
            METRICS_LOGGER.logError(errorMessage, exception)

            writeResponse(message, MessageStatus.RUNTIME, errorMessage)
        } catch (e: Exception) {
            val errorMessage = "Got error during message preprocessing failure handling"
            e.addSuppressed(exception)
            logger.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }
}