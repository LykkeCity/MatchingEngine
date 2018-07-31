
package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
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
        private val databaseAccessor: CashOperationIdDatabaseAccessor): MessagePreprocessor, Thread(CashInOutPreprocessor::class.java.name) {

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

        if (!isDataValid(parsedData)) {
            return
        }

        performDeduplicationCheck(parsedData)
    }

    private fun isDataValid(cashTransferParsedData: CashInOutParsedData): Boolean {
        val parsedMessageWrapper = cashTransferParsedData.messageWrapper
        val cashInOutContext = cashTransferParsedData.messageWrapper.context as CashInOutContext
        try {
            cashInOutOperationInputValidator.performValidation(cashTransferParsedData)
        } catch (e: ValidationException) {
            writeErrorResponse(parsedMessageWrapper, cashInOutContext.cashInOutOperation.id, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return false
        }

        return true
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
        LOGGER.info("Cash in/out operation (${context.id}) for client ${context.clientId}, " +
                "asset ${context.asset!!.assetId}, amount: ${NumberUtils.roundForPrint(context.cashInOutOperation.amount)}: $errorMessage")
    }

    override fun run() {
        while (true) {
            val message = cashInOutQueue.take()
            try {
                preProcess(message)
            } catch (exception: Exception) {
                val context = message.context
                LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message}", exception)

                if (context != null) {
                    LOGGER.error("Error details: $context")
                }

                METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
                writeResponse(message, RUNTIME)
            }
        }
    }
}