package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.CurrentTransactionDataHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.services.*
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.ApplicationContext
import java.util.*
import java.util.concurrent.BlockingQueue

class MessageProcessor(messageRouter: MessageRouter, applicationContext: ApplicationContext)
    : Thread(MessageProcessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val messagesQueue: BlockingQueue<MessageWrapper> = messageRouter.preProcessedMessageQueue

    private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor
    private val persistenceManager: PersistenceManager

    private val cashInOutOperationService: CashInOutOperationService
    private val cashTransferOperationService: CashTransferOperationService
    private val singleLimitOrderService: SingleLimitOrderService
    private val multiLimitOrderService: MultiLimitOrderService
    private val marketOrderService: MarketOrderService
    private val limitOrderCancelService: LimitOrderCancelService
    private val limitOrderMassCancelService: LimitOrderMassCancelService
    private val multiLimitOrderCancelService: MultiLimitOrderCancelService
    private val transferOperationSaveService: TransferOperationSaveService

    private val applicationSettingsHolder: ApplicationSettingsHolder

    private val servicesMap: Map<MessageType, AbstractService>
    private val processedMessagesCache: ProcessedMessagesCache

    private val performanceStatsHolder: PerformanceStatsHolder

    val appInitialData: AppInitialData

    private val reservedCashInOutOperationService: ReservedCashInOutOperationService
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder

    private var currentTransactionDataHolder: CurrentTransactionDataHolder

    init {
        messageProcessingStatusHolder = applicationContext.getBean(MessageProcessingStatusHolder::class.java)
        performanceStatsHolder = applicationContext.getBean(PerformanceStatsHolder::class.java)

        messageSequenceNumberHolder = applicationContext.getBean(MessageSequenceNumberHolder::class.java)

        persistenceManager = applicationContext.getBean("persistenceManager") as PersistenceManager

        cashOperationsDatabaseAccessor = applicationContext.getBean(AzureCashOperationsDatabaseAccessor::class.java)

        this.marketOrderDatabaseAccessor = applicationContext.getBean(AzureMarketOrderDatabaseAccessor::class.java)
        this.backOfficeDatabaseAccessor = applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)

        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)
        this.applicationSettingsHolder = applicationContext.getBean(ApplicationSettingsHolder::class.java)

        val genericLimitOrderService = applicationContext.getBean(GenericLimitOrderService::class.java)
        val genericStopLimitOrderService = applicationContext.getBean(GenericStopLimitOrderService::class.java)

        this.multiLimitOrderService = applicationContext.getBean(MultiLimitOrderService::class.java)
        this.singleLimitOrderService = applicationContext.getBean(SingleLimitOrderService::class.java)

        this.cashInOutOperationService = applicationContext.getBean(CashInOutOperationService::class.java)
        this.reservedCashInOutOperationService = applicationContext.getBean(ReservedCashInOutOperationService::class.java)
        this.cashTransferOperationService = applicationContext.getBean(CashTransferOperationService::class.java)

        this.marketOrderService = applicationContext.getBean(MarketOrderService::class.java)

        this.limitOrderCancelService = applicationContext.getBean(LimitOrderCancelService::class.java)

        this.limitOrderMassCancelService = applicationContext.getBean(LimitOrderMassCancelService::class.java)

        this.multiLimitOrderCancelService = applicationContext.getBean(MultiLimitOrderCancelService::class.java)

        this.transferOperationSaveService = applicationContext.getBean(TransferOperationSaveService::class.java)

        this.currentTransactionDataHolder = applicationContext.getBean(CurrentTransactionDataHolder::class.java)

        processedMessagesCache = applicationContext.getBean(ProcessedMessagesCache::class.java)
        servicesMap = initServicesMap()

        appInitialData = AppInitialData(genericLimitOrderService.initialOrdersCount, genericStopLimitOrderService.initialStopOrdersCount, balanceHolder.initialBalancesCount, balanceHolder.initialClientsCount)
    }

    override fun run() {
        transferOperationSaveService.start()

        while (true) {
            processMessage(messagesQueue.take())
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

            recordPerformanceStats(message, startTime, endTime)
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message processing", exception)
        }
    }

    private fun recordPerformanceStats(messageWrapper: MessageWrapper, startMessageProcessingTime: Long, endMessageProcessingTime: Long) {
        val totalTime = endMessageProcessingTime - messageWrapper.startTimestamp
        val processingTime = endMessageProcessingTime - startMessageProcessingTime

        val inputQueueTime = messageWrapper.messagePreProcessorStartTimestamp?.let {
            it - messageWrapper.startTimestamp
        }

        val preProcessingTime = messageWrapper.messagePreProcessorStartTimestamp?.let {
            messageWrapper.messagePreProcessorEndTimestamp!! - it
        }

        val preProcessedMessageQueueStartTime = messageWrapper.messagePreProcessorEndTimestamp
                ?: messageWrapper.startTimestamp

        val preProcessedMessageQueueTime = startMessageProcessingTime - preProcessedMessageQueueStartTime

        performanceStatsHolder.addMessage(messageWrapper.type,
                inputQueueTime,
                preProcessedMessageQueueTime,
                preProcessingTime,
                processingTime,
                totalTime)
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