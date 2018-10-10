package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.ApplicationContext
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor(config: Config, messageRouter: MessageRouter, applicationContext: ApplicationContext)
    : Thread(MessageProcessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val cashInOutPreprocessor: CashInOutPreprocessor
    private val cashTransferPreprocessor: CashTransferPreprocessor

    private val messagesQueue: BlockingQueue<MessageWrapper> = messageRouter.preProcessedMessageQueue

    private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
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
    private val tradesInfoService: TradesInfoService
    private val historyTicksService: HistoryTicksService
    private val transferOperationSaveService: TransferOperationSaveService

    private val marketStateCache: MarketStateCache
    private val applicationSettingsCache: ApplicationSettingsCache

    private val servicesMap: Map<MessageType, AbstractService>
    private val processedMessagesCache: ProcessedMessagesCache

    private var bestPriceBuilder: Timer? = null
    private var candlesBuilder: Timer? = null
    private var hoursCandlesBuilder: Timer? = null
    private var historyTicksBuilder: Timer? = null

    private val performanceStatsHolder: PerformanceStatsHolder

    val appInitialData: AppInitialData

    private val reservedCashInOutOperationService: ReservedCashInOutOperationService
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder

    init {
        val isLocalProfile = applicationContext.environment.acceptsProfiles("local")
        messageProcessingStatusHolder = applicationContext.getBean(MessageProcessingStatusHolder::class.java)
        performanceStatsHolder = applicationContext.getBean(PerformanceStatsHolder::class.java)

        messageSequenceNumberHolder = applicationContext.getBean(MessageSequenceNumberHolder::class.java)

        this.marketStateCache = applicationContext.getBean(MarketStateCache::class.java)
        persistenceManager = applicationContext.getBean("persistenceManager") as PersistenceManager

        cashOperationsDatabaseAccessor = applicationContext.getBean(AzureCashOperationsDatabaseAccessor::class.java)

        this.limitOrderDatabaseAccessor = applicationContext.getBean(AzureLimitOrderDatabaseAccessor::class.java)
        this.marketOrderDatabaseAccessor = applicationContext.getBean(AzureMarketOrderDatabaseAccessor::class.java)
        this.backOfficeDatabaseAccessor = applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)

        val assetsHolder = applicationContext.getBean(AssetsHolder::class.java)
        val assetsPairsHolder = applicationContext.getBean(AssetsPairsHolder::class.java)
        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)
        this.applicationSettingsCache = applicationContext.getBean(ApplicationSettingsCache::class.java)

        val genericLimitOrderService = applicationContext.getBean(GenericLimitOrderService::class.java)
        val genericStopLimitOrderService = applicationContext.getBean(GenericStopLimitOrderService::class.java)

        this.multiLimitOrderService = applicationContext.getBean(MultiLimitOrderService::class.java)

        val genericLimitOrderProcessorFactory = applicationContext.getBean(GenericLimitOrderProcessorFactory::class.java)
        val genericLimitOrdersCancellerFactory = applicationContext.getBean(GenericLimitOrdersCancellerFactory::class.java)

        this.cashInOutOperationService = applicationContext.getBean(CashInOutOperationService::class.java)
        this.reservedCashInOutOperationService = applicationContext.getBean(ReservedCashInOutOperationService::class.java)
        this.cashTransferOperationService = applicationContext.getBean(CashTransferOperationService::class.java)
        this.singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        this.marketOrderService = applicationContext.getBean(MarketOrderService::class.java)

        this.limitOrderCancelService = applicationContext.getBean(LimitOrderCancelService::class.java)

        this.limitOrderMassCancelService = applicationContext.getBean(LimitOrderMassCancelService::class.java)

        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory, applicationSettingsCache)

        this.tradesInfoService = applicationContext.getBean(TradesInfoService::class.java)

        this.transferOperationSaveService = applicationContext.getBean(TransferOperationSaveService::class.java)

        this.cashInOutPreprocessor = applicationContext.getBean(CashInOutPreprocessor::class.java)
        cashInOutPreprocessor.start()
        this.cashTransferPreprocessor = applicationContext.getBean(CashTransferPreprocessor::class.java)
        cashTransferPreprocessor.start()

        this.historyTicksService = HistoryTicksService(marketStateCache,
                genericLimitOrderService,
                applicationContext.environment.getProperty("application.tick.frequency")!!.toLong())

        if (!isLocalProfile) {
            marketStateCache.refresh()
            this.historyTicksBuilder = historyTicksService.start()
        }

        val connectionsHolder = applicationContext.getBean(ConnectionsHolder::class.java)

        processedMessagesCache = applicationContext.getBean(ProcessedMessagesCache::class.java)
        servicesMap = initServicesMap()

        if (!isLocalProfile) {
            this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = config.me.bestPricesInterval) {
                limitOrderDatabaseAccessor.updateBestPrices(genericLimitOrderService.buildMarketProfile())
            }

            val time = LocalDateTime.now()
            this.candlesBuilder = fixedRateTimer(name = "CandleBuilder", initialDelay = ((1000 - time.nano / 1000000) + 1000 * (63 - time.second)).toLong(), period = config.me.candleSaverInterval) {
                tradesInfoService.saveCandles()
            }

            this.hoursCandlesBuilder = fixedRateTimer(name = "HoursCandleBuilder", initialDelay = 0, period = config.me.hoursCandleSaverInterval) {
                tradesInfoService.saveHourCandles()
            }
        }

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
                LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                METRICS_LOGGER.logError("Unknown message type: ${message.type}")
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
        result[MessageType.CASH_IN_OUT_OPERATION] = cashInOutOperationService
        result[MessageType.CASH_TRANSFER_OPERATION] = cashTransferOperationService
        result[MessageType.RESERVED_CASH_IN_OUT_OPERATION] = reservedCashInOutOperationService
        result[MessageType.LIMIT_ORDER] = singleLimitOrderService
        result[MessageType.OLD_LIMIT_ORDER] = singleLimitOrderService
        result[MessageType.MARKET_ORDER] = marketOrderService
        result[MessageType.LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.OLD_LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.LIMIT_ORDER_MASS_CANCEL] = limitOrderMassCancelService
        result[MessageType.MULTI_LIMIT_ORDER_CANCEL] = multiLimitOrderCancelService
        result[MessageType.MULTI_LIMIT_ORDER] = multiLimitOrderService
        return result
    }
}