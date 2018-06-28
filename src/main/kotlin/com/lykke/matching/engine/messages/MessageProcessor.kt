package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.ProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.JedisHolder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.outgoing.database.TransferOperationSaveService
import com.lykke.matching.engine.outgoing.http.RequestHandler
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.outgoing.socket.SocketServer
import com.lykke.matching.engine.performance.PerformanceStatsHolder
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
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.lykke.matching.engine.utils.monitoring.GeneralHealthMonitor
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpServer
import org.springframework.context.ApplicationContext
import java.net.InetSocketAddress
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Date
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

    private val rabbitTransferQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val dbTransferOperationQueue = LinkedBlockingQueue<TransferOperation>()
    private val balanceUpdateHandler: BalanceUpdateHandler

    private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor
    private val processedMessagesDatabaseAccessor: ProcessedMessagesDatabaseAccessor
    private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor

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

    private var quotesUpdateHandler: QuotesUpdateHandler

    private val servicesMap: Map<MessageType, AbstractService>
    private val notDeduplicateMessageTypes = setOf(MessageType.MULTI_LIMIT_ORDER, MessageType.OLD_MULTI_LIMIT_ORDER, MessageType.MULTI_LIMIT_ORDER_CANCEL)
    private val processedMessagesCache: ProcessedMessagesCache

    private var bestPriceBuilder: Timer? = null
    private var candlesBuilder: Timer? = null
    private var hoursCandlesBuilder: Timer? = null
    private var historyTicksBuilder: Timer? = null

    private val performanceStatsHolder: PerformanceStatsHolder

    val appInitialData: AppInitialData

    private val reservedBalanceUpdateService: ReservedBalanceUpdateService
    private val reservedCashInOutOperationService: ReservedCashInOutOperationService
    private val healthMonitor = GeneralHealthMonitor(listOf(applicationContext.getBean(JedisHolder::class.java)))

    init {
        val isLocalProfile = applicationContext.environment.acceptsProfiles("local")
        performanceStatsHolder = applicationContext.getBean(PerformanceStatsHolder::class.java)

        this.marketStateCache = applicationContext.getBean(MarketStateCache::class.java)

        cashOperationsDatabaseAccessor = applicationContext.getBean(AzureCashOperationsDatabaseAccessor::class.java)

        this.limitOrderDatabaseAccessor = applicationContext.getBean(AzureLimitOrderDatabaseAccessor::class.java)
        this.marketOrderDatabaseAccessor = applicationContext.getBean(AzureMarketOrderDatabaseAccessor::class.java)
        this.backOfficeDatabaseAccessor = applicationContext.getBean(AzureBackOfficeDatabaseAccessor::class.java)
        this.orderBookDatabaseAccessor = applicationContext.getBean(FileOrderBookDatabaseAccessor::class.java)

        balanceUpdateHandler = applicationContext.getBean(BalanceUpdateHandler::class.java)

        val assetsHolder = applicationContext.getBean(AssetsHolder::class.java)
        val assetsPairsHolder = applicationContext.getBean(AssetsPairsHolder::class.java)
        val balanceHolder = applicationContext.getBean(BalancesHolder::class.java)
        this.applicationSettingsCache = applicationContext.getBean(ApplicationSettingsCache::class.java)
        val stopOrderBookDatabaseAccessor = applicationContext.getBean(FileStopOrderBookDatabaseAccessor::class.java)

        this.genericLimitOrderService = applicationContext.getBean(GenericLimitOrderService::class.java)

        val genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderBookDatabaseAccessor, genericLimitOrderService)
        val feeProcessor = FeeProcessor(balanceHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
        this.multiLimitOrderService = applicationContext.getBean(MultiLimitOrderService::class.java)


        val genericLimitOrderProcessorFactory = applicationContext.getBean(GenericLimitOrderProcessorFactory::class.java)
        val genericLimitOrdersCancellerFactory = applicationContext.getBean(GenericLimitOrdersCancellerFactory::class.java)

        this.cashOperationService = applicationContext.getBean(CashOperationService::class.java)
        val cashInOutOperationValidator = applicationContext.getBean(CashInOutOperationValidator::class.java)
        this.cashInOutOperationService = CashInOutOperationService(assetsHolder, balanceHolder, rabbitCashInOutQueue, feeProcessor, cashInOutOperationValidator)
        this.reservedCashInOutOperationService = applicationContext.getBean(ReservedCashInOutOperationService::class.java)
        val cashTransferOperationValidator = applicationContext.getBean(CashTransferOperationValidator::class.java)
        this.cashTransferOperationService = CashTransferOperationService(balanceHolder, assetsHolder, rabbitTransferQueue, dbTransferOperationQueue, feeProcessor, cashTransferOperationValidator)
        this.cashSwapOperationService = applicationContext.getBean(CashSwapOperationService::class.java)
        this.singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        this.marketOrderService = applicationContext.getBean(MarketOrderService::class.java)

        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)

        this.limitOrderMassCancelService = LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)

        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
        this.balanceUpdateService = applicationContext.getBean(BalanceUpdateService::class.java)
        this.reservedBalanceUpdateService = ReservedBalanceUpdateService(balanceHolder)

        this.tradesInfoService = applicationContext.getBean(TradesInfoService::class.java)

        this.historyTicksService = HistoryTicksService(marketStateCache,
                genericLimitOrderService,
                applicationContext.environment.getProperty("application.tick.frequency")!!.toLong())

        if (!isLocalProfile) {
            marketStateCache.refresh()
            this.historyTicksBuilder = historyTicksService.start()
        }

        this.quotesUpdateHandler = applicationContext.getBean(QuotesUpdateHandler::class.java)
        val connectionsHolder = applicationContext.getBean(ConnectionsHolder::class.java)

        processedMessagesDatabaseAccessor = applicationContext.getBean(FileProcessedMessagesDatabaseAccessor::class.java)
        processedMessagesCache = ProcessedMessagesCache(config.me.processedMessagesInterval,
                processedMessagesDatabaseAccessor.loadProcessedMessages(Date.from(LocalDate.now().minusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant())))
        servicesMap = initServicesMap()

        if (config.me.serverOrderBookPort != null) {
            SocketServer(config, connectionsHolder, genericLimitOrderService, assetsHolder, assetsPairsHolder).start()
        }

        val rabbitMqService = applicationContext.getBean(RabbitMqService::class.java)

        val tablePrefix = applicationContext.environment.getProperty("azure.table.prefix", "")
        val logContainer = applicationContext.environment.getProperty("azure.logs.blob.container", "")
        startRabbitMqPublisher(config.me.rabbitMqConfigs.cashOperations, rabbitCashInOutQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineCashOperations", logContainer)),
                rabbitMqService,
                config.me.name,
                AppVersion.VERSION)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.transfers, rabbitTransferQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "${tablePrefix}MatchingEngineTransfers", logContainer)),
                rabbitMqService,
                config.me.name,
                AppVersion.VERSION)

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

        val server = HttpServer.create(InetSocketAddress(config.me.httpOrderBookPort), 0)
        server.createContext("/orderBooks", RequestHandler(genericLimitOrderService))
        server.executor = null
        server.start()

        appInitialData = AppInitialData(genericLimitOrderService.initialOrdersCount, genericStopLimitOrderService.initialStopOrdersCount, balanceHolder.initialBalancesCount, balanceHolder.initialClientsCount)
    }

    private fun startRabbitMqPublisher(config: RabbitConfig,
                                       queue: BlockingQueue<JsonSerializable>,
                                       messageDatabaseLogger: MessageDatabaseLogger? = null,
                                       rabbitMqService: RabbitMqService,
                                       appName: String,
                                       appVersion: String) {
        rabbitMqService.startPublisher(config, queue, appName, appVersion, messageDatabaseLogger)
    }

    override fun run() {
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

            if (!notDeduplicateMessageTypes.contains(messageType)) {
                if (processedMessagesCache.isProcessed(message.type, message.messageId!!)) {
                    service.writeResponse(message, MessageStatus.DUPLICATE)
                    LOGGER.error("Message already processed: ${message.type}: ${message.messageId!!}")
                    METRICS_LOGGER.logError("Message already processed: ${message.type}: ${message.messageId!!}")
                    return
                }
            }

            service.processMessage(message)
            if (!notDeduplicateMessageTypes.contains(messageType)) {
                val processedMessage = ProcessedMessage(message.type, message.timestamp!!, message.messageId!!)
                processedMessagesCache.addMessage(processedMessage)
                processedMessagesDatabaseAccessor.saveProcessedMessage(processedMessage)
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