
package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashInOutPreprocessor(
        private val cashInOutQueue: BlockingQueue<MessageWrapper>,
        private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
        private val databaseAccessor: CashOperationIdDatabaseAccessor,
        private val cashInOutOperationPreprocessorPersistenceManager: PersistenceManager,
        private val processedMessagesCache: ProcessedMessagesCache): MessagePreprocessor, Thread(CashInOutPreprocessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashInOutPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var cashInOutContextParser: CashInOutContextParser

    @Autowired
    private lateinit var cashInOutOperationInputValidator: CashInOutOperationInputValidator

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = cashInOutContextParser.parse(messageWrapper)

        if (!validateData(parsedData)) {
            return
        }

        performDeduplicationCheck(parsedData)
    }

    private fun validateData(cashInOutParsedData: CashInOutParsedData): Boolean {
        try {
            cashInOutOperationInputValidator.performValidation(cashInOutParsedData)
        } catch (e: ValidationException) {
            processInvalidData(cashInOutParsedData, e.validationType, e.message)
            return false
        }

        return true
    }

    private fun processInvalidData(cashInOutParsedData: CashInOutParsedData,
                                   validationType: ValidationException.Validation,
                                   message: String) {
        val messageWrapper = cashInOutParsedData.messageWrapper
        val context = messageWrapper.context as CashInOutContext

        val persistSuccess = cashInOutOperationPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeErrorResponse(messageWrapper, context.cashInOutOperation.matchingEngineOperationId, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            LOGGER.error("Error occurred during processing of invalid cash in/out data, context $context", e)
            METRICS_LOGGER.logError("Error occurred during invalid data processing, ${messageWrapper.type} ${context.messageId}")
        }
    }

    private fun performDeduplicationCheck(cashInOutParsedData: CashInOutParsedData) {
        val parsedMessageWrapper = cashInOutParsedData.messageWrapper
        val context = cashInOutParsedData.messageWrapper.context as CashInOutContext
        if (databaseAccessor.isAlreadyProcessed(parsedMessageWrapper.type.toString(), context.messageId)) {
            writeResponse(parsedMessageWrapper, DUPLICATE)
            LOGGER.info("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
            METRICS_LOGGER.logError("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
        } else {
            preProcessedMessageQueue.put(parsedMessageWrapper)
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   operationId: String,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        val context = messageWrapper.context as CashInOutContext
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(operationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash in/out operation (${context.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${context.cashInOutOperation.clientId}, " +
                "asset ${context.cashInOutOperation.asset!!.assetId}, amount: ${NumberUtils.roundForPrint(context.cashInOutOperation.amount)}: $errorMessage")
    }

    override fun run() {
        while (true) {
            try {
                val message = cashInOutQueue.take()
                try {
                    preProcess(message)
                } catch (exception: Exception) {
                    val context = message.context
                    LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message} " +
                            if (context != null) "Error details: $context" else "", exception)

                    METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
                    writeResponse(message, RUNTIME)
                }
            } catch (e: Exception) {
                val message = "Get error during message preprocessing"
                LOGGER.error(message, e)
                METRICS_LOGGER.logError(message, e)
            }
        }
    }
}