package com.lykke.matching.engine.messages

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
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
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MessageDatabaseLogger
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.outgoing.http.BalancesRequestHandler
import com.lykke.matching.engine.outgoing.http.OrderBooksRequestHandler
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqPublisher
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.outgoing.socket.SocketServer
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
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.utils.QueueSizeLogger
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.RabbitConfig
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.DefaultIsAliveResponseGetter
import com.lykke.utils.keepalive.http.KeepAliveStarter
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.time.LocalDateTime
import java.util.Timer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor(config: Config, queue: BlockingQueue<MessageWrapper>) : Thread() {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageProcessor::class.java.name)
        val MONITORING_LOGGER = ThrottlingLogger.getLogger("${MessageProcessor::class.java.name}.monitoring")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val messagesQueue: BlockingQueue<MessageWrapper> = queue
    val tradesInfoQueue: BlockingQueue<TradeInfo> = LinkedBlockingQueue<TradeInfo>()
    val balanceNotificationQueue: BlockingQueue<BalanceUpdateNotification> = LinkedBlockingQueue<BalanceUpdateNotification>()
    val quotesNotificationQueue: BlockingQueue<QuotesUpdate> = LinkedBlockingQueue<QuotesUpdate>()
    val orderBooksQueue: BlockingQueue<OrderBook> = LinkedBlockingQueue<OrderBook>()
    val balanceUpdatesQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()

    val rabbitOrderBooksQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitTransferQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitCashSwapQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitCashInOutQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitSwapQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitTrustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()
    val rabbitClientLimitOrdersQueue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue<JsonSerializable>()

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor
    val orderBookDatabaseAccessor: OrderBookDatabaseAccessor

    val cashOperationService: CashOperationService
    val cashInOutOperationService: CashInOutOperationService
    val cashTransferOperationService: CashTransferOperationService
    val cashSwapOperationService: CashSwapOperationService
    val genericLimitOrderService: GenericLimitOrderService
    val singleLimitOrderService: SingleLimitOrderService
    val multiLimitOrderService: MultiLimitOrderService
    val marketOrderService: MarketOrderService
    val limitOrderCancelService: LimitOrderCancelService
    val multiLimitOrderCancelService: MultiLimitOrderCancelService
    val balanceUpdateService: BalanceUpdateService
    val tradesInfoService: TradesInfoService
    val historyTicksService: HistoryTicksService

    val balanceUpdateHandler: BalanceUpdateHandler
    val quotesUpdateHandler: QuotesUpdateHandler

    val bestPriceBuilder: Timer
    val candlesBuilder: Timer
    val hoursCandlesBuilder: Timer
    val historyTicksBuilder: Timer
    val appInitialData: AppInitialData

    init {
        val cashOperationsDatabaseAccessor = AzureCashOperationsDatabaseAccessor(config.me.db.balancesInfoConnString)
        this.walletDatabaseAccessor = createWalletDatabaseAccessor(config.me)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config.me.db.hLiquidityConnString)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(config.me.db.hTradesConnString)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString)
        this.historyTicksDatabaseAccessor = AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString)
        this.orderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.fileDb.orderBookPath)
        val assetsHolder = AssetsHolder(AssetsCache(AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString), 60000))
        val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(AzureDictionariesDatabaseAccessor(config.me.db.dictsConnString), 60000))
        val balanceHolder = BalancesHolder(walletDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdatesQueue, config.me.trustedClients)
        this.cashOperationService = CashOperationService(cashOperationsDatabaseAccessor, balanceHolder)
        this.cashInOutOperationService = CashInOutOperationService(cashOperationsDatabaseAccessor, assetsHolder, balanceHolder, rabbitCashInOutQueue)
        this.cashTransferOperationService = CashTransferOperationService(balanceHolder, assetsHolder, cashOperationsDatabaseAccessor, rabbitTransferQueue)
        this.cashSwapOperationService = CashSwapOperationService(balanceHolder, assetsHolder, cashOperationsDatabaseAccessor, rabbitCashSwapQueue)
        this.genericLimitOrderService = GenericLimitOrderService(orderBookDatabaseAccessor, assetsHolder, assetsPairsHolder, balanceHolder, tradesInfoQueue, quotesNotificationQueue, config.me.trustedClients)
        this.singleLimitOrderService = SingleLimitOrderService(genericLimitOrderService, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, assetsHolder, assetsPairsHolder, config.me.negativeSpreadAssets.split(";").toSet(), balanceHolder, marketOrderDatabaseAccessor)
        this.multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, assetsHolder, assetsPairsHolder, config.me.negativeSpreadAssets.split(";").toSet(), balanceHolder, marketOrderDatabaseAccessor)
        this.marketOrderService = MarketOrderService(backOfficeDatabaseAccessor, marketOrderDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balanceHolder, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, orderBooksQueue, rabbitOrderBooksQueue, rabbitSwapQueue)
        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, rabbitClientLimitOrdersQueue, assetsHolder, assetsPairsHolder, balanceHolder, orderBooksQueue, rabbitOrderBooksQueue)
        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, orderBooksQueue, rabbitTrustedClientsLimitOrdersQueue, rabbitClientLimitOrdersQueue, rabbitOrderBooksQueue)
        this.balanceUpdateService = BalanceUpdateService(balanceHolder)
        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)
        this.historyTicksService = HistoryTicksService(historyTicksDatabaseAccessor, genericLimitOrderService)
        historyTicksService.init()
        this.balanceUpdateHandler = BalanceUpdateHandler(balanceNotificationQueue)
        balanceUpdateHandler.start()
        this.quotesUpdateHandler = QuotesUpdateHandler(quotesNotificationQueue)
        quotesUpdateHandler.start()
        val connectionsHolder = ConnectionsHolder(orderBooksQueue)
        connectionsHolder.start()

        if (config.me.serverOrderBookPort != null) {
            SocketServer(config, connectionsHolder, genericLimitOrderService, assetsHolder, assetsPairsHolder).start()
        }

        startRabbitMqPublisher(config.me.rabbitMqConfigs.orderBooks, rabbitOrderBooksQueue)

        startRabbitMqPublisher(config.me.rabbitMqConfigs.cashOperations, rabbitCashInOutQueue,
                MessageDatabaseLogger(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString, "MatchingEngineCashOperations")))

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

        KeepAliveStarter.start(config.keepAlive, DefaultIsAliveResponseGetter(), AppVersion.VERSION)

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

        while (true) {
            processMessage(messagesQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
        try {
            val messageType = MessageType.Companion.valueOf(message.type)
            when (messageType) {
            //MessageType.PING -> already processed by client handler
                MessageType.CASH_OPERATION -> {
                    cashOperationService.processMessage(message)
                }
                MessageType.CASH_IN_OUT_OPERATION -> {
                    cashInOutOperationService.processMessage(message)
                }
                MessageType.CASH_TRANSFER_OPERATION -> {
                    cashTransferOperationService.processMessage(message)
                }
                MessageType.CASH_SWAP_OPERATION -> {
                    cashSwapOperationService.processMessage(message)
                }
                MessageType.LIMIT_ORDER,
                MessageType.OLD_LIMIT_ORDER -> {
                    singleLimitOrderService.processMessage(message)
                }
                MessageType.MARKET_ORDER,
                MessageType.NEW_MARKET_ORDER,
                MessageType.OLD_MARKET_ORDER -> {
                    marketOrderService.processMessage(message)
                }
                MessageType.LIMIT_ORDER_CANCEL,
                MessageType.OLD_LIMIT_ORDER_CANCEL -> {
                    limitOrderCancelService.processMessage(message)
                }
                MessageType.MULTI_LIMIT_ORDER_CANCEL -> {
                    multiLimitOrderCancelService.processMessage(message)
                }
                MessageType.OLD_BALANCE_UPDATE,
                MessageType.BALANCE_UPDATE -> {
                    balanceUpdateService.processMessage(message)
                }
                MessageType.MULTI_LIMIT_ORDER,
                MessageType.OLD_MULTI_LIMIT_ORDER -> {
                    multiLimitOrderService.processMessage(message)
                }
                MessageType.BALANCE_UPDATE_SUBSCRIBE -> {
                    balanceUpdateHandler.subscribe(message.clientHandler!!)
                }
                MessageType.QUOTES_UPDATE_SUBSCRIBE -> {
                    quotesUpdateHandler.subscribe(message.clientHandler!!)
                }
                else -> {
                    LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                    METRICS_LOGGER.logError( "Unknown message type: ${message.type}")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError( "[${message.sourceIp}]: Got error during message processing", exception)
        }
    }
}