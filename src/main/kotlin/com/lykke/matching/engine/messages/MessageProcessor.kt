package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.order.ExecutionDataApplyService
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

    private val cashInOutPreprocessor: CashInOutPreprocessor
    private val cashTransferPreprocessor: CashTransferPreprocessor

    private val messagesQueue: BlockingQueue<MessageWrapper> = messageRouter.preProcessedMessageQueue

    private val balanceUpdateHandler: BalanceUpdateHandler

    private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor
    private val persistenceManager: PersistenceManager

    private val cashOperationService: CashOperationService
    private val cashInOutOperationService: CashInOutOperationService
    private val cashTransferOperationService: CashTransferOperationService
    private val cashSwapOperationService: CashSwapOperationService
    private val singleLimitOrderService: SingleLimitOrderService
    private val multiLimitOrderService: MultiLimitOrderService
    private val marketOrderService: MarketOrderService
    private val limitOrderCancelService: LimitOrderCancelService
    private val limitOrderMassCancelService: LimitOrderMassCancelService
    private val multiLimitOrderCancelService: MultiLimitOrderCancelService
    private val balanceUpdateService: BalanceUpdateService
    private val transferOperationSaveService: TransferOperationSaveService

    private val applicationSettingsHolder: ApplicationSettingsHolder

    private val quotesUpdateHandler: QuotesUpdateHandler

    private val servicesMap: Map<MessageType, AbstractService>
    private val processedMessagesCache: ProcessedMessagesCache

    private val performanceStatsHolder: PerformanceStatsHolder

    val appInitialData: AppInitialData

    private val reservedBalanceUpdateService: ReservedBalanceUpdateService
    private val reservedCashInOutOperationService: ReservedCashInOutOperationService
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder

    init {
        messageProcessingStatusHolder = applicationContext.getBean(MessageProcessingStatusHolder::class.java)
        performanceStatsHolder = applicationContext.getBean(PerformanceStatsHolder::class.java)

        messageSequenceNumberHolder = applicationContext.getBean(MessageSequenceNumberHolder::class.java)

        persistenceManager = applicationContext.getBean("persistenceManager") as PersistenceManager

        cashOperationsDatabaseAccessor = applicationContext.getBean(AzureCashOperationsDatabaseAccessor::class.java)

        this.marketOrderDatabaseAccessor = applicationContext.getBean(AzureMarketOrderDatabaseAccessor::class.java)
        this.backOfficeDatabaseAccessor = applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)

        balanceUpdateHandler = applicationContext.getBean(BalanceUpdateHandler::class.java)

        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)
        this.applicationSettingsHolder = applicationContext.getBean(ApplicationSettingsHolder::class.java)

        val genericLimitOrderService = applicationContext.getBean(GenericLimitOrderService::class.java)
        val genericStopLimitOrderService = applicationContext.getBean(GenericStopLimitOrderService::class.java)

        this.multiLimitOrderService = applicationContext.getBean(MultiLimitOrderService::class.java)

        val genericLimitOrdersCancellerFactory = applicationContext.getBean(GenericLimitOrdersCancellerFactory::class.java)
        val executionContextFactory = applicationContext.getBean(ExecutionContextFactory::class.java)

        this.cashOperationService = applicationContext.getBean(CashOperationService::class.java)
        this.cashInOutOperationService = applicationContext.getBean(CashInOutOperationService::class.java)
        this.reservedCashInOutOperationService = applicationContext.getBean(ReservedCashInOutOperationService::class.java)
        this.cashTransferOperationService = applicationContext.getBean(CashTransferOperationService::class.java)
        this.cashSwapOperationService = applicationContext.getBean(CashSwapOperationService::class.java)
        this.singleLimitOrderService = SingleLimitOrderService(executionContextFactory,
                applicationContext.getBean(GenericLimitOrdersProcessor::class.java),
                applicationContext.getBean(StopOrderBookProcessor::class.java),
                applicationContext.getBean(ExecutionDataApplyService::class.java),
                applicationContext.getBean(PreviousLimitOrdersProcessor::class.java))

        this.marketOrderService = applicationContext.getBean(MarketOrderService::class.java)

        this.limitOrderCancelService = applicationContext.getBean(LimitOrderCancelService::class.java)

        this.limitOrderMassCancelService = applicationContext.getBean(LimitOrderMassCancelService::class.java)

        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory, applicationSettingsHolder)
        this.balanceUpdateService = applicationContext.getBean(BalanceUpdateService::class.java)
        this.reservedBalanceUpdateService = ReservedBalanceUpdateService(balanceHolder)

        this.transferOperationSaveService = applicationContext.getBean(TransferOperationSaveService::class.java)

        this.cashInOutPreprocessor = applicationContext.getBean(CashInOutPreprocessor::class.java)
        cashInOutPreprocessor.start()
        this.cashTransferPreprocessor = applicationContext.getBean(CashTransferPreprocessor::class.java)
        cashTransferPreprocessor.start()

        this.quotesUpdateHandler = applicationContext.getBean(QuotesUpdateHandler::class.java)

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

            val service = servicesMap[messageType]

            if (service == null) {
                when (messageType) {
                    MessageType.BALANCE_UPDATE_SUBSCRIBE -> {
                        balanceUpdateHandler.subscribe(message.clientHandler!!)
                    }
                    MessageType.QUOTES_UPDATE_SUBSCRIBE -> {
                        quotesUpdateHandler.subscribe(message.clientHandler!!)
                    }
                    else -> {
                        LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                        METRICS_LOGGER.logError("Unknown message type: ${message.type}")
                    }
                }
                return
            }

            if (message.parsedMessage == null) {
                service.parseMessage(message)
            }


            if (!messageProcessingStatusHolder.isMessageSwitchEnabled()) {
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

            performanceStatsHolder.addMessage(message.type, endTime - message.startTimestamp, endTime - startTime)
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message processing", exception)
        }
    }

    private fun initServicesMap(): Map<MessageType, AbstractService> {
        val result = HashMap<MessageType, AbstractService>()
        result[MessageType.CASH_OPERATION] = cashOperationService
        result[MessageType.CASH_IN_OUT_OPERATION] = cashInOutOperationService
        result[MessageType.CASH_TRANSFER_OPERATION] = cashTransferOperationService
        result[MessageType.CASH_SWAP_OPERATION] = cashSwapOperationService
        result[MessageType.RESERVED_CASH_IN_OUT_OPERATION] = reservedCashInOutOperationService
        result[MessageType.LIMIT_ORDER] = singleLimitOrderService
        result[MessageType.OLD_LIMIT_ORDER] = singleLimitOrderService
        result[MessageType.MARKET_ORDER] = marketOrderService
        result[MessageType.LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.OLD_LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.LIMIT_ORDER_MASS_CANCEL] = limitOrderMassCancelService
        result[MessageType.MULTI_LIMIT_ORDER_CANCEL] = multiLimitOrderCancelService
        result[MessageType.OLD_BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.RESERVED_BALANCE_UPDATE] = reservedBalanceUpdateService
        result[MessageType.MULTI_LIMIT_ORDER] = multiLimitOrderService
        return result
    }
}