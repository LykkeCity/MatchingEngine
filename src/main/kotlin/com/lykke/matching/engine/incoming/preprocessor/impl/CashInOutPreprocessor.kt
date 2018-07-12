package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.context.ApplicationContext
import java.util.concurrent.BlockingQueue

class CashInOutPreprocessor(
        private val incomingQueue: BlockingQueue<MessageWrapper>,
        private val outgoingQueue: BlockingQueue<MessageWrapper>,
        private val databaseAccessor: CashOperationIdDatabaseAccessor,
        private val applicationContext: ApplicationContext
) : MessagePreprocessor, Thread(CashInOutPreprocessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashInOutPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val cashInOutContextParser = applicationContext.getBean(CashInOutContextParser::class.java)
        val parsedMessageWrapper = cashInOutContextParser.parse(messageWrapper)
        val context = parsedMessageWrapper.context as CashInOutContext

        if (!isDataValid(parsedMessageWrapper, context)) {
            return
        }

        performDeduplicationCheck(parsedMessageWrapper, context)
    }

    private fun isDataValid(parsedMessageWrapper: MessageWrapper, cashInOutContext: CashInOutContext): Boolean {
        val cashInOutOperationValidator = applicationContext.getBean(CashInOutOperationValidator::class.java)

        try {
            cashInOutOperationValidator.performValidation(cashInOutContext)
        } catch (e: ValidationException) {
            writeErrorResponse(parsedMessageWrapper, cashInOutContext.walletOperation.id, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return false
        }

        return true
    }

    private fun performDeduplicationCheck(parsedMessageWrapper: MessageWrapper, context: CashInOutContext) {
        if (databaseAccessor.isAlreadyProcessed(parsedMessageWrapper.type.toString(), context.messageId)) {
            writeResponse(parsedMessageWrapper, DUPLICATE)
            LOGGER.info("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
            METRICS_LOGGER.logError("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
        } else {
            outgoingQueue.put(parsedMessageWrapper)
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
                "asset ${context.asset!!.assetId}, amount: ${NumberUtils.roundForPrint(context.walletOperation.amount)}: $errorMessage")
    }

    override fun run() {
        while (true) {
            val message = incomingQueue.take()
            try {
                preProcess(message)
            } catch (exception: Exception) {
                LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message}", exception)
                METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
                writeResponse(message, RUNTIME)
            }
        }
    }
}