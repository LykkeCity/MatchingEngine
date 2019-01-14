package com.lykke.matching.engine.messages

import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.CurrentTransactionDataHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.services.*
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*
import javax.annotation.PostConstruct

@Component
class MessageProcessor : Thread(MessageProcessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var messageRouter: MessageRouter

    @Autowired
    private lateinit var persistenceManager: PersistenceManager

    @Autowired
    private lateinit var cashInOutOperationService: CashInOutOperationService

    @Autowired
    private lateinit var cashTransferOperationService: CashTransferOperationService

    @Autowired
    private lateinit var singleLimitOrderService: SingleLimitOrderService

    @Autowired
    private lateinit var multiLimitOrderService: MultiLimitOrderService

    @Autowired
    private lateinit var marketOrderService: MarketOrderService

    @Autowired
    private lateinit var limitOrderCancelService: LimitOrderCancelService

    @Autowired
    private lateinit var limitOrderMassCancelService: LimitOrderMassCancelService

    @Autowired
    private lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    @Autowired
    private lateinit var transferOperationSaveService: TransferOperationSaveService

    @Autowired
    private lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService

    @Autowired
    private lateinit var messageProcessingStatusHolder: MessageProcessingStatusHolder

    @Autowired
    private lateinit var currentTransactionDataHolder: CurrentTransactionDataHolder

    @Autowired
    private lateinit var processedMessagesCache: ProcessedMessagesCache

    @Autowired
    private lateinit var performanceStatsHolder: PerformanceStatsHolder

    @Autowired
    private lateinit var messageSequenceNumberHolder: MessageSequenceNumberHolder

    private lateinit var servicesMap: Map<MessageType, AbstractService>

    @PostConstruct
    private fun init() {
        servicesMap = initServicesMap()
    }

    override fun run() {
        transferOperationSaveService.start()

        while (true) {
            processMessage(messageRouter.preProcessedMessageQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
        val startTime = System.nanoTime()
        try {
            val messageType = MessageType.valueOf(message.type)
            if (messageType == null) {
                LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                METRICS_LOGGER.logError("Unknown message type: ${message.type}")
                return
            }

            currentTransactionDataHolder.setMessageType(messageType)

            val service = servicesMap[messageType]

            if (service == null) {
                LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                METRICS_LOGGER.logError("Unknown message type: ${message.type}")
                return
            }

            if (message.parsedMessage == null) {
                service.parseMessage(message)
            }

            if (!messageProcessingStatusHolder.isMessageProcessingEnabled()) {
                service.writeResponse(message, MessageStatus.MESSAGE_PROCESSING_DISABLED)
                return
            }

            if (!messageProcessingStatusHolder.isHealthStatusOk()) {
                service.writeResponse(message, MessageStatus.RUNTIME)
                val errorMessage = "Message processing is disabled"
                LOGGER.error(errorMessage)
                METRICS_LOGGER.logError(errorMessage)
                return
            }

            val processedMessage = message.processedMessage
            if (processedMessage != null && processedMessagesCache.isProcessed(processedMessage.type, processedMessage.messageId)) {
                service.writeResponse(message, MessageStatus.DUPLICATE)
                LOGGER.error("Message already processed: ${message.type}: ${message.messageId!!}")
                METRICS_LOGGER.logError("Message already processed: ${message.type}: ${message.messageId!!}")
                return
            }

            service.processMessage(message)

            processedMessage?.let {
                if (!message.triedToPersist) {
                    message.persisted = persistenceManager.persist(PersistenceData(it, messageSequenceNumberHolder.getValueToPersist()))
                }
                if (message.persisted) {
                    processedMessagesCache.addMessage(it)
                }
            }

            val endTime = System.nanoTime()

            if (message.writeResponseTime == null) {
                val errorMessage = "There was no write response to socket time recorded, response to socket is not written, messageId: ${message.messageId}"
                LOGGER.error(errorMessage)
                METRICS_LOGGER.logError(errorMessage)
            }

            performanceStatsHolder.addMessage(type = message.type,
                    writeResponseTime = message.writeResponseTime,
                    startTimestamp = message.startTimestamp,
                    messagePreProcessorStartTimestamp = message.messagePreProcessorStartTimestamp,
                    messagePreProcessorEndTimestamp = message.messagePreProcessorEndTimestamp,
                    startMessageProcessingTime = startTime,
                    endMessageProcessingTime = endTime)
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message processing", exception)
        }
    }

    private fun initServicesMap(): Map<MessageType, AbstractService> {
        val result = HashMap<MessageType, AbstractService>()
        result[MessageType.CASH_IN_OUT_OPERATION] = cashInOutOperationService
        result[MessageType.CASH_TRANSFER_OPERATION] = cashTransferOperationService
        result[MessageType.RESERVED_CASH_IN_OUT_OPERATION] = reservedCashInOutOperationService
        result[MessageType.LIMIT_ORDER] = singleLimitOrderService
        result[MessageType.MARKET_ORDER] = marketOrderService
        result[MessageType.LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.LIMIT_ORDER_MASS_CANCEL] = limitOrderMassCancelService
        result[MessageType.MULTI_LIMIT_ORDER_CANCEL] = multiLimitOrderCancelService
        result[MessageType.MULTI_LIMIT_ORDER] = multiLimitOrderService
        return result
    }
}