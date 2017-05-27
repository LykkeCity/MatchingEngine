package com.lykke.matching.engine.messages

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.SharedDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureSharedDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.outgoing.http.RequestHandler
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqPublisher
import com.lykke.matching.engine.outgoing.socket.ConnectionsHolder
import com.lykke.matching.engine.outgoing.socket.SocketServer
import com.lykke.matching.engine.queue.BackendQueueProcessor
import com.lykke.matching.engine.queue.QueueWriter
import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import com.lykke.matching.engine.queue.transaction.Transaction
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
import com.lykke.matching.engine.services.WalletCredentialsCacheService
import com.lykke.matching.engine.utils.AppVersion
import com.lykke.matching.engine.utils.QueueSizeLogger
import com.lykke.matching.engine.utils.config.Config
import com.sun.net.httpserver.HttpServer
import org.apache.log4j.Logger
import java.net.InetSocketAddress
import java.time.LocalDateTime
import java.util.Date
import java.util.Timer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor(config: Config, queue: BlockingQueue<MessageWrapper>) : Thread() {

    companion object {
        val LOGGER = Logger.getLogger(MessageProcessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val messagesQueue: BlockingQueue<MessageWrapper> = queue
    val bitcoinQueue: BlockingQueue<Transaction> = LinkedBlockingQueue<Transaction>()
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

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor
    val sharedDatabaseAccessor: SharedDatabaseAccessor
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
    val walletCredentialsService: WalletCredentialsCacheService

    val balanceUpdateHandler: BalanceUpdateHandler
    val quotesUpdateHandler: QuotesUpdateHandler

    val backendQueueProcessor: BackendQueueProcessor
    val azureQueueWriter: QueueWriter

    val walletCredentialsCache: WalletCredentialsCache

    val bestPriceBuilder: Timer
    val candlesBuilder: Timer
    val hoursCandlesBuilder: Timer
    val historyTicksBuilder: Timer

    init {
        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.dictsConnString)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config.me.db.aLimitOrdersConnString, config.me.db.hLimitOrdersConnString, config.me.db.hLiquidityConnString)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(config.me.db.hMarketOrdersConnString, config.me.db.hTradesConnString)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(config.me.db.multisigConnString, config.me.db.bitCoinQueueConnectionString, config.me.db.dictsConnString)
        this.historyTicksDatabaseAccessor = AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString)
        this.sharedDatabaseAccessor = AzureSharedDatabaseAccessor(config.me.db.sharedStorageConnString)
        this.orderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.orderBookPath)
        this.azureQueueWriter = AzureQueueWriter(config.me.db.bitCoinQueueConnectionString, config.me.backendQueueName ?: "indata")
        this.walletCredentialsCache = WalletCredentialsCache(backOfficeDatabaseAccessor)
        val assetsHolder = AssetsHolder(AssetsCache(AzureBackOfficeDatabaseAccessor(config.me.db.multisigConnString, config.me.db.bitCoinQueueConnectionString, config.me.db.dictsConnString), 60000))
        val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.dictsConnString), 60000))
        val balanceHolder = BalancesHolder(walletDatabaseAccessor, assetsHolder, balanceNotificationQueue)
        this.cashOperationService = CashOperationService(walletDatabaseAccessor, bitcoinQueue, balanceHolder, balanceUpdatesQueue)
        this.cashInOutOperationService = CashInOutOperationService(walletDatabaseAccessor, assetsHolder, balanceHolder, rabbitCashInOutQueue, balanceUpdatesQueue)
        this.cashTransferOperationService = CashTransferOperationService(balanceHolder, assetsHolder, walletDatabaseAccessor, rabbitTransferQueue, balanceUpdatesQueue)
        this.cashSwapOperationService = CashSwapOperationService(balanceHolder, assetsHolder, walletDatabaseAccessor, rabbitCashSwapQueue, balanceUpdatesQueue)
        this.genericLimitOrderService = GenericLimitOrderService(config.me.useFileOrderBook, limitOrderDatabaseAccessor, orderBookDatabaseAccessor, assetsPairsHolder, balanceHolder, tradesInfoQueue, quotesNotificationQueue)
        this.singleLimitOrderService = SingleLimitOrderService(this.genericLimitOrderService, orderBooksQueue, rabbitOrderBooksQueue, assetsPairsHolder, config.me.negativeSpreadAssets.split(";").toSet())
        this.multiLimitOrderService = MultiLimitOrderService(this.genericLimitOrderService, orderBooksQueue, rabbitOrderBooksQueue, assetsPairsHolder, config.me.negativeSpreadAssets.split(";").toSet())
        this.marketOrderService = MarketOrderService(backOfficeDatabaseAccessor, marketOrderDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balanceHolder, bitcoinQueue, orderBooksQueue, rabbitOrderBooksQueue, walletCredentialsCache,
                config.me.lykkeTradesHistoryEnabled, rabbitSwapQueue, balanceUpdatesQueue, config.me.publishToRabbitQueue, config.me.sendTrades)
        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService)
        this.multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, orderBooksQueue, rabbitOrderBooksQueue)
        this.balanceUpdateService = BalanceUpdateService(balanceHolder, balanceUpdatesQueue)
        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)
        this.walletCredentialsService = WalletCredentialsCacheService(walletCredentialsCache)
        this.historyTicksService = HistoryTicksService(historyTicksDatabaseAccessor, genericLimitOrderService)
        historyTicksService.init()
        this.balanceUpdateHandler = BalanceUpdateHandler(balanceNotificationQueue)
        balanceUpdateHandler.start()
        this.quotesUpdateHandler = QuotesUpdateHandler(quotesNotificationQueue)
        quotesUpdateHandler.start()
        this.backendQueueProcessor = BackendQueueProcessor(backOfficeDatabaseAccessor, bitcoinQueue, azureQueueWriter, walletCredentialsCache)
        val connectionsHolder = ConnectionsHolder(orderBooksQueue)
        connectionsHolder.start()

        SocketServer(config, connectionsHolder, genericLimitOrderService, assetsHolder, assetsPairsHolder).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeOrderbook, rabbitOrderBooksQueue).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeTransfer, rabbitTransferQueue).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeSwapOperation, rabbitCashSwapQueue).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeCashOperation, rabbitCashInOutQueue).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeSwap, rabbitSwapQueue).start()
        RabbitMqPublisher(config.me.rabbit.host, config.me.rabbit.port, config.me.rabbit.username,
                config.me.rabbit.password, config.me.rabbit.exchangeBalanceUpdate, balanceUpdatesQueue).start()

        this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = config.me.bestPricesInterval) {
            limitOrderDatabaseAccessor.updateBestPrices(genericLimitOrderService.buildMarketProfile())
        }

        val time = LocalDateTime.now()
        this.candlesBuilder = fixedRateTimer(name = "CandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second)).toLong(), period = config.me.candleSaverInterval) {
            tradesInfoService.saveCandles()
        }

        this.hoursCandlesBuilder = fixedRateTimer(name = "HoursCandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second) + 60000 * (60 - time.minute)).toLong(), period = config.me.hoursCandleSaverInterval) {
            tradesInfoService.saveHourCandles()
        }

        this.historyTicksBuilder = fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = (60 * 60 * 1000) / 4000) {
            historyTicksService.buildTicks()
        }

        val queueSizeLogger = QueueSizeLogger(messagesQueue, orderBooksQueue, rabbitOrderBooksQueue, config.me.queueSizeLimit)
        fixedRateTimer(name = "QueueSizeLogger", initialDelay = config.me.queueSizeLoggerInterval, period = config.me.queueSizeLoggerInterval) {
            queueSizeLogger.log()
        }
        fixedRateTimer(name = "StatusUpdater", initialDelay = 0, period = 30000) {
            sharedDatabaseAccessor.updateKeepAlive(Date(), AppVersion.VERSION)
        }

        val server = HttpServer.create(InetSocketAddress(config.me.httpOrderBookPort), 0)
        server.createContext("/orderBooks", RequestHandler(genericLimitOrderService))
        server.executor = null
        server.start()
    }

    override fun run() {
        backendQueueProcessor.start()
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
                MessageType.BALANCE_UPDATE -> {
                    balanceUpdateService.processMessage(message)
                }
                MessageType.MULTI_LIMIT_ORDER,
                MessageType.OLD_MULTI_LIMIT_ORDER -> {
                    multiLimitOrderService.processMessage(message)
                }
                MessageType.WALLET_CREDENTIALS_RELOAD -> {
                    walletCredentialsService.processMessage(message)
                }
                MessageType.BALANCE_UPDATE_SUBSCRIBE -> {
                    balanceUpdateHandler.subscribe(message.clientHandler!!)
                }
                MessageType.QUOTES_UPDATE_SUBSCRIBE -> {
                    quotesUpdateHandler.subscribe(message.clientHandler!!)
                }
                else -> {
                    LOGGER.error("[${message.sourceIp}]: Unknown message type: ${message.type}")
                    METRICS_LOGGER.logError(this.javaClass.name, "Unknown message type: ${message.type}")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("[${message.sourceIp}]: Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError(this.javaClass.name, "[${message.sourceIp}]: Got error during message processing", exception)
        }
    }
}