package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.ProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMonitoringDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.database.createWalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.outgoing.database.LkkTradeSaveService
import com.lykke.matching.engine.outgoing.http.BalancesRequestHandler
import com.lykke.matching.engine.outgoing.http.OrderBooksRequestHandler
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqPublisher
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
import com.lykke.matching.engine.services.HistoryTicksService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderCancelService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.ReservedBalanceUpdateService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.utils.QueueSizeLogger
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.DefaultIsAliveResponseGetter
import com.lykke.utils.keepalive.http.KeepAliveStarter
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpServer
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

class MessageProcessor(config: Config, queue: BlockingQueue<MessageWrapper>) : Thread(MessageProcessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }
  
    private val messagesQueue: BlockingQueue<MessageWrapper> = queue
    private val tradesInfoQueue: BlockingQueue<TradeInfo> = LinkedBlockingQueue<TradeInfo>()
    private val balanceNotificationQueue: BlockingQueue<BalanceUpdateNotification> = LinkedBlockingQueue<BalanceUpdateNotification>()
    private val quotesNotificationQueue: BlockingQueue<QuotesUpdate> = LinkedBlockingQueue<QuotesUpdate>()
    private val orderBooksQueue: BlockingQueue<OrderBook> = LinkedBlockingQueue<OrderBook>()
    private val balanceUpdatesQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()

    private val rabbitOrderBooksQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitTransferQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitCashSwapQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitReservedCashInOutQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitSwapQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitTrustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val rabbitClientLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    private val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    private val walletDatabaseAccessor: WalletDatabaseAccessor
    private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    private val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor
    private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor

    private val cashOperationService: CashOperationService
    private val cashInOutOperationService: CashInOutOperationService
    private val cashTransferOperationService: CashTransferOperationService
    private val cashSwapOperationService: CashSwapOperationService
    private val genericLimitOrderService: GenericLimitOrderService
    private val singleLimitOrderService: SingleLimitOrderService
    private val multiLimitOrderService: MultiLimitOrderService
    private val marketOrderService: MarketOrderService
    private val limitOrderCancelService: LimitOrderCancelService
    private val multiLimitOrderCancelService: MultiLimitOrderCancelService
    private val balanceUpdateService: BalanceUpdateService
    private val tradesInfoService: TradesInfoService
    private val historyTicksService: HistoryTicksService

    private val balanceUpdateHandler: BalanceUpdateHandler
    private val quotesUpdateHandler: QuotesUpdateHandler

    private val servicesMap: Map<MessageType, AbstractService>
    private val notDeduplicateMessageTypes = setOf(MessageType.MULTI_LIMIT_ORDER, MessageType.OLD_MULTI_LIMIT_ORDER, MessageType.MULTI_LIMIT_ORDER_CANCEL)
    private val processedMessagesCache: ProcessedMessagesCache
    private val processedMessagesDatabaseAccessor: ProcessedMessagesDatabaseAccessor

    private val bestPriceBuilder: Timer
    private val candlesBuilder: Timer
    private val hoursCandlesBuilder: Timer
    private val historyTicksBuilder: Timer

    private val performanceStatsHolder = PerformanceStatsHolder()

    val appInitialData: AppInitialData

    private val reservedBalanceUpdateService: ReservedBalanceUpdateService
    private val reservedCashInOutOperationService: ReservedCashInOutOperationService

    init {
        val cashOperationsDatabaseAccessor = AzureCashOperationsDatabaseAccessor(config.me.db.balancesInfoConnString)
        this.walletDatabaseAccessor = createWalletDatabaseAccessor(config.me, true)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config.me.db.hLiquidityConnString)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(config.me.db.hTradesConnString)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString)
        this.historyTicksDatabaseAccessor = AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString)
        this.orderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.fileDb.orderBookPath)
        val assetsHolder = AssetsHolder(AssetsCache(AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString), 60000))
        val dictionariesDatabaseAccessor = AzureDictionariesDatabaseAccessor(config.me.db.dictsConnString)
        val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(dictionariesDatabaseAccessor, 60000))
        val balanceHolder = BalancesHolder(walletDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdatesQueue, config.me.trustedClients)
        this.genericLimitOrderService = GenericLimitOrderService(orderBookDatabaseAccessor, assetsHolder, assetsPairsHolder, balanceHolder, tradesInfoQueue, quotesNotificationQueue, config.me.trustedClients)
        val feeProcessor = FeeProcessor(balanceHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)

        this.cashOperationService = CashOperationService(balanceHolder)
        this.cashInOutOperationService = CashInOutOperationService(cashOperationsDatabaseAccessor, assetsHolder, balanceHolder, rabbitCashInOutQueue, feeProcessor)
        this.reservedCashInOutOperationService = ReservedCashInOutOperationService(assetsHolder, balanceHolder, rabbitReservedCashInOutQueue)
        this.cashTransferOperationService = CashTransferOperationService(balanceHolder, assetsHolder, cashOperationsDatabaseAccessor, rabbitTransferQueue, feeProcessor)
        this.cashSwapOperationService = CashSwapOperationService(balanceHolder, assetsHolder, cashOperationsDatabaseAccessor, rabbitCashSwapQueue)
        this.singleLimitOrderService = SingleLimitOrderService(genericLimitOrderService, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, assetsHolder, assetsPairsHolder, balanceHolder, lkkTradesQueue)
        this.multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, assetsHolder, assetsPairsHolder, balanceHolder, lkkTradesQueue)
        this.marketOrderService = MarketOrderService(backOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balanceHolder, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, rabbitSwapQueue, lkkTradesQueue)
        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, rabbitClientLimitOrdersQueue, assetsHolder, assetsPairsHolder, balanceHolder, orderBooksQueue, rabbitOrderBooksQueue)
        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, orderBooksQueue, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, rabbitOrderBooksQueue)
        this.balanceUpdateService = BalanceUpdateService(balanceHolder)
        this.reservedBalanceUpdateService = ReservedBalanceUpdateService(balanceHolder)

        if (config.me.cancelMinVolumeOrders) {
            MinVolumeOrderCanceller(dictionariesDatabaseAccessor, assetsPairsHolder, balanceHolder, genericLimitOrderService, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue).cancel()
        }

        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)
        this.historyTicksService = HistoryTicksService(historyTicksDatabaseAccessor, genericLimitOrderService)
        historyTicksService.init()
        this.balanceUpdateHandler = BalanceUpdateHandler(balanceNotificationQueue)
        balanceUpdateHandler.start()
        this.quotesUpdateHandler = QuotesUpdateHandler(quotesNotificationQueue)
        quotesUpdateHandler.start()
        val connectionsHolder = ConnectionsHolder(orderBooksQueue)
        connectionsHolder.start()

        processedMessagesDatabaseAccessor = FileProcessedMessagesDatabaseAccessor(config.me.fileDb.processedMessagesPath)
        processedMessagesCache = ProcessedMessagesCache(config.me.fileDb.processedMessagesInterval, processedMessagesDatabaseAccessor.loadProcessedMessages(Date.from(LocalDate.now().minusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant())))
        servicesMap = initServicesMap()

        if (config.me.serverOrderBookPort != null) {
            SocketServer(config, connectionsHolder, genericLimitOrderService, assetsHolder, assetsPairsHolder).start()
        }

        startRabbitMqPublisher(config.me.rabbitMqConfigs.orderBooks, rabbitOrderBooksQueue)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.cashOperations, rabbitCashInOutQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineCashOperations")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.reservedCashOperations, rabbitReservedCashInOutQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineReservedCashOperations")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.transfers, rabbitTransferQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineTransfers")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.swapOperations, rabbitCashSwapQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineSwapOperations")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.balanceUpdates, balanceUpdatesQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineBalanceUpdates")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.marketOrders, rabbitSwapQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineMarketOrders")))

        startRabbitMqPublisher(config.me.rabbitMqConfigs.limitOrders, rabbitTrustedClientsLimitOrdersQueue)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.trustedLimitOrders, rabbitClientLimitOrdersQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineLimitOrders")))

        this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = config.me.bestPricesInterval) {
            limitOrderDatabaseAccessor.updateBestPrices(genericLimitOrderService.buildMarketProfile())
        }

        val time = LocalDateTime.now()
        this.candlesBuilder = fixedRateTimer(name = "CandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second)).toLong(), period = config.me.candleSaverInterval) {
            tradesInfoService.saveCandles()
        }

        this.hoursCandlesBuilder = fixedRateTimer(name = "HoursCandleBuilder", initialDelay = 0, period = config.me.hoursCandleSaverInterval) {
            tradesInfoService.saveHourCandles()
        }

        this.historyTicksBuilder = fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = (60 * 60 * 1000) / 4000) {
            historyTicksService.buildTicks()
        }

        val queueSizeLogger = QueueSizeLogger(messagesQueue, orderBooksQueue, rabbitOrderBooksQueue, config.me.queueSizeLimit)
        fixedRateTimer(name = "QueueSizeLogger", initialDelay = config.me.queueSizeLoggerInterval, period = config.me.queueSizeLoggerInterval) {
            queueSizeLogger.log()
        }

        val healthService = MonitoringStatsCollector()
        val monitoringDatabaseAccessor = AzureMonitoringDatabaseAccessor(config.me.db.monitoringConnString)
        fixedRateTimer(name = "Monitoring", initialDelay = 5 * 60 * 1000, period = 5 * 60 * 1000) {
            val result = healthService.collectMonitoringResult()
            if (result != null) {
                MONITORING_LOGGER.info("CPU: ${RoundingUtils.roundForPrint2(result.vmCpuLoad)}/${RoundingUtils.roundForPrint2(result.totalCpuLoad)}, " +
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

        KeepAliveStarter.start(config.me.keepAlive, DefaultIsAliveResponseGetter(), AppVersion.VERSION)

        val orderBooksServer = HttpServer.create(InetSocketAddress(config.me.httpOrderBookPort), 0)
        orderBooksServer.createContext("/orderBooks", OrderBooksRequestHandler(genericLimitOrderService))
        orderBooksServer.executor = null
        orderBooksServer.start()

        val balancesServer = HttpServer.create(InetSocketAddress(config.me.httpBalancesPort), 0)
        balancesServer.createContext("/balances", BalancesRequestHandler(balanceHolder))
        balancesServer.executor = null
        balancesServer.start()

        appInitialData = AppInitialData(genericLimitOrderService.initialOrdersCount, balanceHolder.initialBalancesCount, balanceHolder.initialClientsCount)
    }

    private fun startRabbitMqPublisher(config: RabbitConfig, queue: BlockingQueue<JsonSerializable>, messageDatabaseLogger: MessageDatabaseLogger? = null) {
        RabbitMqPublisher(config.uri, config.exchange, queue, messageDatabaseLogger).start()
    }

    override fun run() {
        tradesInfoService.start()
        LkkTradeSaveService(marketOrderDatabaseAccessor, lkkTradesQueue).start()

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
        result[MessageType.MULTI_LIMIT_ORDER_CANCEL] = multiLimitOrderCancelService
        result[MessageType.OLD_BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.BALANCE_UPDATE] = balanceUpdateService
        result[MessageType.RESERVED_BALANCE_UPDATE] = reservedBalanceUpdateService
        result[MessageType.MULTI_LIMIT_ORDER] = multiLimitOrderService
        result[MessageType.OLD_MULTI_LIMIT_ORDER] = multiLimitOrderService
        return result
    }
}