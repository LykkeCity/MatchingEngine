package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.keepalive.MeIsAliveResponseGetter
import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.database.LkkTradeSaveService
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.outgoing.http.RequestHandler
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.outgoing.socket.SocketServer
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.performance.PerformanceStatsLogger
import com.lykke.matching.engine.services.AbstractService
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.CashInOutOperationService
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.CashSwapOperationService
import com.lykke.matching.engine.services.CashTransferOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.HistoryTicksService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.LimitOrderMassCancelService
import com.lykke.matching.engine.services.MultiLimitOrderCancelService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.ReservedBalanceUpdateService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.utils.QueueSizeLogger
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.lykke.matching.engine.utils.monitoring.GeneralHealthMonitor
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.KeepAliveStarter
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpServer
import org.springframework.context.ApplicationContext
import java.net.InetSocketAddress
import java.time.LocalDateTime
import java.util.HashMap
import java.util.Timer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor(config: Config, queue: BlockingQueue<MessageWrapper>, applicationContext: ApplicationContext)
    : Thread(MessageProcessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val messagesQueue: BlockingQueue<MessageWrapper> = queue
    private val tradesInfoQueue: BlockingQueue<TradeInfo> = LinkedBlockingQueue<TradeInfo>()
    private val quotesNotificationQueue: BlockingQueue<QuotesUpdate> = LinkedBlockingQueue<QuotesUpdate>()
    private val orderBooksQueue: BlockingQueue<OrderBook> = LinkedBlockingQueue<OrderBook>()

    private val rabbitOrderBooksQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitTransferQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitSwapQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitTrustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitClientLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    private val dbTransferOperationQueue = LinkedBlockingQueue<TransferOperation>()
    private val balanceUpdateHandler: BalanceUpdateHandler

    private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor
    private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor
    private val persistenceManager: PersistenceManager

    private val cashOperationService: CashOperationService
    private val cashInOutOperationService: CashInOutOperationService
    private val cashTransferOperationService: CashTransferOperationService
    private val cashSwapOperationService: CashSwapOperationService
    private val genericLimitOrderService: GenericLimitOrderService
    private val singleLimitOrderService: SingleLimitOrderService
    private val multiLimitOrderService: MultiLimitOrderService
    private val marketOrderService: MarketOrderService
    private val limitOrderCancelService: LimitOrderCancelService
    private val limitOrderMassCancelService: LimitOrderMassCancelService
    private val multiLimitOrderCancelService: MultiLimitOrderCancelService
    private val balanceUpdateService: BalanceUpdateService
    private val tradesInfoService: TradesInfoService
    private val historyTicksService: HistoryTicksService

    private val marketStateCache: MarketStateCache
    private val applicationSettingsCache: ApplicationSettingsCache

    private val quotesUpdateHandler: QuotesUpdateHandler

    private val servicesMap: Map<MessageType, AbstractService>
    private val processedMessagesCache: ProcessedMessagesCache

    private var bestPriceBuilder: Timer? = null
    private var candlesBuilder: Timer? = null
    private var hoursCandlesBuilder: Timer? = null
    private var historyTicksBuilder: Timer? = null

    private val performanceStatsHolder = PerformanceStatsHolder()

    val appInitialData: AppInitialData

    private val reservedBalanceUpdateService: ReservedBalanceUpdateService
    private val reservedCashInOutOperationService: ReservedCashInOutOperationService
    private val healthMonitor: GeneralHealthMonitor

    init {
        val isLocalProfile = applicationContext.environment.acceptsProfiles("local")
        healthMonitor = applicationContext.getBean(GeneralHealthMonitor::class.java)
        this.marketStateCache = applicationContext.getBean(MarketStateCache::class.java)
        persistenceManager = applicationContext.getBean(PersistenceManager::class.java)

        cashOperationsDatabaseAccessor = applicationContext.getBean(AzureCashOperationsDatabaseAccessor::class.java)

        this.limitOrderDatabaseAccessor = applicationContext.getBean(AzureLimitOrderDatabaseAccessor::class.java)
        this.marketOrderDatabaseAccessor = applicationContext.getBean(AzureMarketOrderDatabaseAccessor::class.java)
        this.backOfficeDatabaseAccessor =  applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)
        this.orderBookDatabaseAccessor = applicationContext.getBean(FileOrderBookDatabaseAccessor::class.java)

        balanceUpdateHandler = applicationContext.getBean(BalanceUpdateHandler::class.java)

        val assetsHolder = applicationContext.getBean(AssetsHolder::class.java)
        val dictionariesDatabaseAccessor = applicationContext.getBean(DictionariesDatabaseAccessor::class.java)
        val assetsPairsHolder = applicationContext.getBean(AssetsPairsHolder::class.java)
        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)
        this.applicationSettingsCache = applicationContext.getBean(ApplicationSettingsCache::class.java)
        val stopOrderBookDatabaseAccessor = FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath)

        this.genericLimitOrderService = GenericLimitOrderService(orderBookDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balanceHolder,
                tradesInfoQueue,
                quotesNotificationQueue, applicationSettingsCache)

        val genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderBookDatabaseAccessor, genericLimitOrderService)
        val feeProcessor = FeeProcessor(balanceHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)

        val limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder,
                assetsPairsHolder,
                balanceHolder,
                genericLimitOrderService,
                applicationSettingsCache,
                rabbitTrustedClientsLimitOrdersQueue,
                rabbitClientLimitOrdersQueue,
                lkkTradesQueue,
                orderBooksQueue,
                rabbitOrderBooksQueue)

        val genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                rabbitClientLimitOrdersQueue,
                assetsHolder,
                assetsPairsHolder,
                balanceHolder,
                applicationSettingsCache)

        val genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor,
                assetsPairsHolder,
                balanceHolder,
                genericLimitOrderService,
                genericStopLimitOrderService,
                genericLimitOrderProcessorFactory,
                rabbitTrustedClientsLimitOrdersQueue,
                rabbitClientLimitOrdersQueue,
                orderBooksQueue,
                rabbitOrderBooksQueue)

        this.cashOperationService = applicationContext.getBean(CashOperationService::class.java)
        val cashInOutOperationValidator = applicationContext.getBean(CashInOutOperationValidator::class.java)
        this.cashInOutOperationService = CashInOutOperationService(assetsHolder, balanceHolder, rabbitCashInOutQueue, feeProcessor, cashInOutOperationValidator)
        this.reservedCashInOutOperationService = applicationContext.getBean(ReservedCashInOutOperationService::class.java)
        val cashTransferOperationValidator = applicationContext.getBean(CashTransferOperationValidator::class.java)
        this.cashTransferOperationService = CashTransferOperationService(balanceHolder, assetsHolder,  rabbitTransferQueue, dbTransferOperationQueue, feeProcessor, cashTransferOperationValidator)
        this.cashSwapOperationService = applicationContext.getBean(CashSwapOperationService::class.java)
        this.singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)


        val multiLimitOrderValidator = applicationContext.getBean(MultiLimitOrderValidator::class.java)
        this.multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrdersProcessorFactory, rabbitTrustedClientsLimitOrdersQueue,
                rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, assetsHolder, assetsPairsHolder, balanceHolder, lkkTradesQueue, genericLimitOrderProcessorFactory, multiLimitOrderValidator)
        val marketOrderValidator = applicationContext.getBean(MarketOrderValidator::class.java)
        this.marketOrderService = MarketOrderService(backOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder,
                assetsPairsHolder, balanceHolder, rabbitTrustedClientsLimitOrdersQueue,
                rabbitClientLimitOrdersQueue,
                orderBooksQueue, rabbitOrderBooksQueue, rabbitSwapQueue, lkkTradesQueue, genericLimitOrderProcessorFactory, marketOrderValidator)

        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)

        this.limitOrderMassCancelService = LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)

        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
        this.balanceUpdateService = applicationContext.getBean(BalanceUpdateService::class.java)
        this.reservedBalanceUpdateService = ReservedBalanceUpdateService(balanceHolder)

        if (config.me.cancelMinVolumeOrders) {
            MinVolumeOrderCanceller(dictionariesDatabaseAccessor, assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory).cancel()
        }

        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)

        this.historyTicksService = HistoryTicksService(marketStateCache,
                genericLimitOrderService,
                applicationContext.environment.getProperty("application.tick.frequency")!!.toLong())

        if (!isLocalProfile) {
            marketStateCache.refresh()
            this.historyTicksBuilder = historyTicksService.start()
        }

        this.quotesUpdateHandler = QuotesUpdateHandler(quotesNotificationQueue)
        quotesUpdateHandler.start()
        val connectionsHolder = ConnectionsHolder(orderBooksQueue)
        connectionsHolder.start()

        processedMessagesCache = applicationContext.getBean(ProcessedMessagesCache::class.java)
        servicesMap = initServicesMap()

        if (config.me.serverOrderBookPort != null) {
            SocketServer(config, connectionsHolder, genericLimitOrderService, assetsHolder, assetsPairsHolder).start()
        }

        val rabbitMqService = applicationContext.getBean(RabbitMqService::class.java)

        startRabbitMqPublisher (config.me.rabbitMqConfigs.orderBooks, rabbitOrderBooksQueue, null, rabbitMqService)

        val tablePrefix = applicationContext.environment.getProperty("azure.table.prefix", "")
        val logContainer = applicationContext.environment.getProperty("azure.logs.blob.container", "")
        startRabbitMqPublisher(config.me.rabbitMqConfigs.cashOperations, rabbitCashInOutQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineCashOperations", logContainer)), rabbitMqService)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.transfers, rabbitTransferQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineTransfers", logContainer)), rabbitMqService)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.marketOrders, rabbitSwapQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineMarketOrders", logContainer)), rabbitMqService)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.limitOrders, rabbitTrustedClientsLimitOrdersQueue, null, rabbitMqService)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.trustedLimitOrders, rabbitClientLimitOrdersQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineLimitOrders", logContainer)), rabbitMqService)

        if(!isLocalProfile) {
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

            val queueSizeLogger = QueueSizeLogger(messagesQueue, orderBooksQueue, rabbitOrderBooksQueue, persistenceManager, config.me.queueSizeLimit)
            fixedRateTimer(name = "QueueSizeLogger", initialDelay = config.me.queueSizeLoggerInterval, period = config.me.queueSizeLoggerInterval) {
                queueSizeLogger.log()
            }

            val healthService = MonitoringStatsCollector()
            val monitoringDatabaseAccessor = applicationContext.getBean(MonitoringDatabaseAccessor::class.java)
            fixedRateTimer(name = "Monitoring", initialDelay = 5 * 60 * 1000, period = 5 * 60 * 1000) {
                val result = healthService.collectMonitoringResult()
                if (result != null) {
                    MONITORING_LOGGER.info("CPU: ${NumberUtils.roundForPrint2(result.vmCpuLoad)}/${NumberUtils.roundForPrint2(result.totalCpuLoad)}, " +
                            "RAM: ${result.freeMemory}/${result.totalMemory}, " +
                            "heap: ${result.freeHeap}/${result.totalHeap}/${result.maxHeap}, " +
                            "swap: ${result.freeSwap}/${result.totalSwap}, " +
                            "threads: ${result.threadsCount}")

                    monitoringDatabaseAccessor.saveMonitoringResult(result)
                }
            }

            val performanceStatsLogger = PerformanceStatsLogger(monitoringDatabaseAccessor)
            fixedRateTimer(name = "PerformanceStatsLogger", initialDelay = config.me.performanceStatsInterval, period = config.me.performanceStatsInterval) {
                performanceStatsLogger.logStats(performanceStatsHolder.getStatsAndReset().values)
            }

            KeepAliveStarter.start(config.me.keepAlive, MeIsAliveResponseGetter(healthMonitor), AppVersion.VERSION)
        }

        val server = HttpServer.create(InetSocketAddress(config.me.httpOrderBookPort), 0)
        server.createContext("/orderBooks", RequestHandler(genericLimitOrderService))
        server.executor = null
        server.start()

        appInitialData = AppInitialData(genericLimitOrderService.initialOrdersCount, genericStopLimitOrderService.initialStopOrdersCount, balanceHolder.initialBalancesCount, balanceHolder.initialClientsCount)
    }

    private fun startRabbitMqPublisher(config: RabbitConfig,
                                       queue: BlockingQueue<JsonSerializable>,
                                       messageDatabaseLogger: MessageDatabaseLogger? = null,
                                       rabbitMqService : RabbitMqService) {
        rabbitMqService.startPublisher (config, queue, messageDatabaseLogger)
    }

    override fun run() {
        tradesInfoService.start()
        LkkTradeSaveService(marketOrderDatabaseAccessor, lkkTradesQueue).start()
        TransferOperationSaveService(cashOperationsDatabaseAccessor, dbTransferOperationQueue).start()

        while (true) {
            processMessage(messagesQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
        val startTime = System.nanoTime()
        try {
            val messageType = MessageType.Companion.valueOf(message.type)
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

            service.parseMessage(message)

            if (!healthMonitor.ok()) {
                service.writeResponse(message, MessageStatus.RUNTIME)
                val errorMessage = "Message processing is disabled"
                LOGGER.error(errorMessage)
                METRICS_LOGGER.logError(errorMessage)
                return
            }

            if (processedMessagesCache.isProcessed(message.type, message.messageId!!)) {
                service.writeResponse(message, MessageStatus.DUPLICATE)
                LOGGER.error("Message already processed: ${message.type}: ${message.messageId!!}")
                METRICS_LOGGER.logError("Message already processed: ${message.type}: ${message.messageId!!}")
                return
            }

            service.processMessage(message)

            message.processedMessage()?.let {
                processedMessagesCache.addMessage(it)
                if (!message.processedMessagePersisted) {
                    persistenceManager.persist(PersistenceData(it))
                }
            }

            val endTime = System.nanoTime()

            performanceStatsHolder.addMessage(message.type, endTime - message.startTimestamp, endTime - startTime)
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError( "[${message.sourceIp}]: Got error during message processing", exception)
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
        result[MessageType.NEW_MARKET_ORDER] = marketOrderService
        result[MessageType.OLD_MARKET_ORDER] = marketOrderService
        result[MessageType.LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.OLD_LIMIT_ORDER_CANCEL] = limitOrderCancelService
        result[MessageType.LIMIT_ORDER_MASS_CANCEL] = limitOrderMassCancelService
        result[MessageType.MULTI_LIMIT_ORDER_CANCEL] = multiLimitOrderCancelService
        result[MessageType.OLD_BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.RESERVED_BALANCE_UPDATE] = reservedBalanceUpdateService
        result[MessageType.MULTI_LIMIT_ORDER] = multiLimitOrderService
        result[MessageType.OLD_MULTI_LIMIT_ORDER] = multiLimitOrderService
        return result
    }
}