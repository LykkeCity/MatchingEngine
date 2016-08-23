package com.lykke.matching.engine.messages

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.notification.BalanceUpdateHandler
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.notification.QuotesUpdateHandler
import com.lykke.matching.engine.queue.BackendQueueProcessor
import com.lykke.matching.engine.queue.QueueWriter
import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import com.lykke.matching.engine.queue.transaction.Transaction
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.HistoryTicksService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.services.WalletCredentialsCacheService
import com.lykke.matching.engine.utils.QueueSizeLogger
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.HashMap
import java.util.Properties
import java.util.Timer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor: Thread {

    companion object {
        val LOGGER = Logger.getLogger(MessageProcessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val messagesQueue: BlockingQueue<MessageWrapper>
    val bitcoinQueue: BlockingQueue<Transaction>
    val tradesInfoQueue: BlockingQueue<TradeInfo>
    val balanceNotificationQueue: BlockingQueue<BalanceUpdateNotification>
    val quotesNotificationQueue: BlockingQueue<QuotesUpdate>

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor
    val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor

    val cashOperationService: CashOperationService
    val genericLimitOrderService: GenericLimitOrderService
    val sinlgeLimitOrderService: SingleLimitOrderService
    val multiLimitOrderService: MultiLimitOrderService
    val marketOrderService: MarketOrderService
    val limitOrderCancelService: LimitOrderCancelService
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

    constructor(config: Properties, azureConfig: HashMap<String, Any>, queue: BlockingQueue<MessageWrapper>) {

        val dbConfig = azureConfig["Db"] as Map<String, String>

        this.messagesQueue = queue
        this.bitcoinQueue = LinkedBlockingQueue<Transaction>()
        this.tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
        this.balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
        this.quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()

        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(dbConfig["BalancesInfoConnString"]!!, dbConfig["DictsConnString"]!!)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(dbConfig["ALimitOrdersConnString"]!!, dbConfig["HLimitOrdersConnString"]!!, dbConfig["HLiquidityConnString"]!!)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(dbConfig["HMarketOrdersConnString"]!!, dbConfig["HTradesConnString"]!!)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(dbConfig["ClientPersonalInfoConnString"]!!, dbConfig["BitCoinQueueConnectionString"]!!, dbConfig["DictsConnString"]!!)
        this.historyTicksDatabaseAccessor = AzureHistoryTicksDatabaseAccessor(dbConfig["HLiquidityConnString"]!!)
        this.azureQueueWriter = AzureQueueWriter(dbConfig["BitCoinQueueConnectionString"]!!, azureConfig)

        this.walletCredentialsCache = WalletCredentialsCache(backOfficeDatabaseAccessor)

        this.cashOperationService = CashOperationService(walletDatabaseAccessor, backOfficeDatabaseAccessor, bitcoinQueue, balanceNotificationQueue)
        this.genericLimitOrderService = GenericLimitOrderService(limitOrderDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        this.sinlgeLimitOrderService = SingleLimitOrderService(this.genericLimitOrderService)
        this.multiLimitOrderService = MultiLimitOrderService(this.genericLimitOrderService)
        this.marketOrderService = MarketOrderService(backOfficeDatabaseAccessor, marketOrderDatabaseAccessor, genericLimitOrderService, cashOperationService, bitcoinQueue, walletCredentialsCache)
        this.limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService)
        this.balanceUpdateService = BalanceUpdateService(cashOperationService)
        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)
        this.walletCredentialsService = WalletCredentialsCacheService(walletCredentialsCache)

        this.historyTicksService = HistoryTicksService(historyTicksDatabaseAccessor, genericLimitOrderService)
        historyTicksService.init()

        this.balanceUpdateHandler = BalanceUpdateHandler(balanceNotificationQueue)
        balanceUpdateHandler.start()

        this.quotesUpdateHandler = QuotesUpdateHandler(quotesNotificationQueue)
        quotesUpdateHandler.start()

        this.backendQueueProcessor = BackendQueueProcessor(backOfficeDatabaseAccessor, bitcoinQueue, azureQueueWriter, walletCredentialsCache)

        val bestPricesInterval = config.getProperty("best.prices.interval")!!.toLong()
        this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = bestPricesInterval) {
            limitOrderDatabaseAccessor.updateBestPrices(genericLimitOrderService.buildMarketProfile())
        }

        val time = LocalDateTime.now()
        val candleSaverInterval = config.getProperty("candle.saver.interval")!!.toLong()
        this.candlesBuilder = fixedRateTimer(name = "CandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second)).toLong(), period = candleSaverInterval) {
            tradesInfoService.saveCandles()
        }

        val hoursCandleSaverInterval = config.getProperty("hours.candle.saver.interval")!!.toLong()
        this.hoursCandlesBuilder = fixedRateTimer(name = "HoursCandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second) + 60000 * (60 - time.minute)).toLong(), period = hoursCandleSaverInterval) {
            tradesInfoService.saveHourCandles()
        }

        this.historyTicksBuilder = fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = (60 * 60 * 1000) / 4000) {
            historyTicksService.buildTicks()
        }

        val queueSizeLogger = QueueSizeLogger(messagesQueue)
        fixedRateTimer(name = "QueueSizeLogger", initialDelay = 300000, period = 300000) {
            queueSizeLogger.log()
        }
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
                MessageType.LIMIT_ORDER -> {
                    sinlgeLimitOrderService.processMessage(message)
                }
                MessageType.MARKET_ORDER -> {
                    marketOrderService.processMessage(message)
                }
                MessageType.LIMIT_ORDER_CANCEL -> {
                    limitOrderCancelService.processMessage(message)
                }
                MessageType.BALANCE_UPDATE -> {
                    balanceUpdateService.processMessage(message)
                }
                MessageType.MULTI_LIMIT_ORDER -> {
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
                    LOGGER.error("Unknown message type: ${message.type}")
                    METRICS_LOGGER.logError(this.javaClass.name, "Unknown message type: ${message.type}")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Got error during message processing: ${exception.message}", exception)
            METRICS_LOGGER.logError(this.javaClass.name, "Got error during message processing: ${exception.message}", exception)
        }
    }
}