package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.impl.input.CashTransferOperationInputValidatorImpl
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.context.ApplicationContext
import java.util.concurrent.BlockingQueue

class CashTransferPreprocessor(
        private val incomingQueue: BlockingQueue<MessageWrapper>,
        private val outgoingQueue: BlockingQueue<MessageWrapper>,
        private val databaseAccessor: CashOperationIdDatabaseAccessor,
        private val applicationContext: ApplicationContext
): MessagePreprocessor, Thread(CashTransferPreprocessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashTransferPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val contextParser = applicationContext.getBean(CashTransferContextParser::class.java)
        val parsedMessageWrapper = contextParser.parse(messageWrapper)
        val context = parsedMessageWrapper.context as CashTransferContext

        if (!isDataValid(parsedMessageWrapper, context)) {
            return
        }

        performDeduplicationCheck(parsedMessageWrapper, context)
    }

    fun isDataValid(parsedMessageWrapper: MessageWrapper, context: CashTransferContext): Boolean {
        val validator = applicationContext.getBean(CashTransferOperationInputValidatorImpl::class.java)

        try {
            validator.performValidation(context)
        } catch (e: ValidationException) {
            writeErrorResponse(parsedMessageWrapper, context, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return false
        }

        return true
    }

    private fun performDeduplicationCheck(parsedMessageWrapper: MessageWrapper, context: CashTransferContext) {
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
                                   context: CashTransferContext,
                                   status: MessageStatus,
                                   errorMessage: String = StringUtils.EMPTY) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(context.transferOperation.id)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
        LOGGER.info("Cash transfer operation (${context.transferOperation.externalId}) from client ${context.transferOperation.fromClientId} " +
                "to client ${context.transferOperation.toClientId}, asset ${context.asset}," +
                " volume: ${NumberUtils.roundForPrint(context.transferOperation.volume)}: $errorMessage")
    }

    override fun run() {
        while (true) {
            val message = incomingQueue.take()
            try {
                preProcess(message)
            } catch (exception: Exception) {
                LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[${message.sourceIp}]: Got error during message preprocessing", exception)
                writeResponse(message, RUNTIME)
            }
        }
    }
}