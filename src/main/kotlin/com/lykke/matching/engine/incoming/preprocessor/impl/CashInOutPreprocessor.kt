package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
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
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue

@Component
class CashInOutPreprocessor(cashInOutContextParser: CashInOutContextParser,
                            preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                            private val cashOperationIdDatabaseAccessor: CashOperationIdDatabaseAccessor,
                            private val cashInOutOperationPreprocessorPersistenceManager: PersistenceManager,
                            private val processedMessagesCache: ProcessedMessagesCache,
                            private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                            @Qualifier("cashInOutPreProcessingLogger")
                            private val logger: ThrottlingLogger) :
        AbstractMessagePreprocessor<CashInOutParsedData>(cashInOutContextParser,
                messageProcessingStatusHolder,
                preProcessedMessageQueue,
                logger) {

    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var cashInOutOperationInputValidator: CashInOutOperationInputValidator

    override fun preProcessParsedData(parsedData: CashInOutParsedData): Boolean {
        val parsedMessageWrapper = parsedData.messageWrapper
        val context = parsedMessageWrapper.context as CashInOutContext
        if ((isCashIn(context.cashInOutOperation.amount) && messageProcessingStatusHolder.isCashInDisabled(context.cashInOutOperation.asset)) ||
                (!isCashIn(context.cashInOutOperation.amount) && messageProcessingStatusHolder.isCashOutDisabled(context.cashInOutOperation.asset))) {
            writeResponse(parsedData.messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

        if (!validateData(parsedData)) {
            return false
        }

        if (isMessageDuplicated(parsedData)) {
            writeResponse(parsedMessageWrapper, DUPLICATE)
            val errorMessage = "Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}"
            logger.info(errorMessage)
            METRICS_LOGGER.logError(errorMessage)
            return false
        }

        return true
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
        logger.info("Input validation failed messageId: ${context.messageId}, details: $message")

        val persistSuccess = cashInOutOperationPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeErrorResponse(messageWrapper, context.cashInOutOperation.matchingEngineOperationId, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid cash in/out data, context $context", e)
            METRICS_LOGGER.logError("Error occurred during invalid data processing, ${messageWrapper.type} ${context.messageId}")
        }
    }

    private fun isMessageDuplicated(cashInOutParsedData: CashInOutParsedData): Boolean {
        val parsedMessageWrapper = cashInOutParsedData.messageWrapper
        val context = cashInOutParsedData.messageWrapper.context as CashInOutContext
        return cashOperationIdDatabaseAccessor.isAlreadyProcessed(parsedMessageWrapper.type.toString(), context.messageId)
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
        logger.info("Cash in/out operation (${context.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${context.cashInOutOperation.clientId}, " +
                "asset ${context.cashInOutOperation.asset!!.assetId}, amount: ${NumberUtils.roundForPrint(context.cashInOutOperation.amount)}: $errorMessage")
    }

    private fun isCashIn(amount: BigDecimal): Boolean {
        return amount > BigDecimal.ZERO
    }
}