package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.LimitOrderCancelOperationInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class LimitOrderCancelOperationPreprocessor(val limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData>,
                                            val limitOrderCancelOperationValidator: LimitOrderCancelOperationInputValidator,
                                            val limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
                                            val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                            val limitOrderCancelOperationPreprocessorPersistenceManager: PersistenceManager,
                                            val processedMessagesCache: ProcessedMessagesCache) : MessagePreprocessor, Thread(LimitOrderCancelOperationPreprocessor::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LimitOrderCancelOperationPreprocessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderCancelOperationContextParser.parse(messageWrapper)
        val parsedMessageWrapper = parsedData.messageWrapper

        if (!validateData(parsedData)) {
            return
        }

        preProcessedMessageQueue.put(parsedMessageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)

        message?.let {
            builder.statusReason = message
        }

        messageWrapper.writeNewResponse(builder)
    }

    override fun run() {
        while (true) {
            val messageWrapper = limitOrderCancelInputQueue.take()
            try {
                preProcess(messageWrapper)
            } catch (e: Exception) {
                handlePreprocessingException(e, messageWrapper)
            }
        }
    }

    private fun processInvalidData(data: LimitOrderCancelOperationParsedData,
                                   validationType: ValidationException.Validation,
                                   message: String) {
        val messageWrapper = data.messageWrapper
        val context = messageWrapper.context as LimitOrderCancelOperationContext
        LOGGER.info("Input validation failed messageId: ${context.messageId}, details: $message")

        val persistenceSuccess = limitOrderCancelOperationPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))

        if (!persistenceSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            LOGGER.error("Error occurred during processing of invalid limit order cancel data, context $context", e)
            METRICS_LOGGER.logError("Error occurred during invalid data processing, ${messageWrapper.type} ${context.messageId}")
        }
    }

    private fun validateData(data: LimitOrderCancelOperationParsedData): Boolean {
        try {
            limitOrderCancelOperationValidator.performValidation(data)
        } catch (e: ValidationException) {
            processInvalidData(data, e.validationType, e.message)
            return false
        }
        return true
    }

    @PostConstruct
    private fun init() {
        this.start()
    }

    private fun handlePreprocessingException(exception: Exception, message: MessageWrapper) {
        try {
            val context = message.context
            LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message} " +
                    if (context != null) "Error details: $context" else "", exception)

            METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
            writeResponse(message, MessageStatus.RUNTIME)
        } catch (e: Exception) {
            val errorMessage = "Got error during message preprocessing failure handling"
            e.addSuppressed(exception)
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }
}