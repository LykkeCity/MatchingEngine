package com.lykke.matching.engine.messages

import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.queue.BackendQueueProcessor
import com.lykke.matching.engine.queue.QueueWriter
import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import com.lykke.matching.engine.queue.transaction.Transaction
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.LimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.TradesInfoService
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Properties
import java.util.Timer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MessageProcessor: Thread {

    companion object {
        val LOGGER = Logger.getLogger(MessageProcessor::class.java.name)
    }

    val messagesQueue: BlockingQueue<MessageWrapper>
    val bitcoinQueue: BlockingQueue<Transaction>
    val tradesInfoQueue: BlockingQueue<TradeInfo>

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor

    val cashOperationService: CashOperationService
    val limitOrderService: LimitOrderService
    val marketOrderService: MarketOrderService
    val limitOrderCancelService: LimitOrderCancelService
    val balanceUpdateService: BalanceUpdateService
    val tradesInfoService: TradesInfoService

    val backendQueueProcessor: BackendQueueProcessor
    val azureQueueWriter: QueueWriter

    val bestPriceBuilder: Timer
    val candlesBuilder: Timer

    constructor(config: Properties, dbConfig: Map<String, String>, queue: BlockingQueue<MessageWrapper>) {
        this.messagesQueue = queue
        this.bitcoinQueue = LinkedBlockingQueue<Transaction>()
        this.tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(dbConfig["BalancesInfoConnString"]!!, dbConfig["DictsConnString"]!!)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(dbConfig["ALimitOrdersConnString"]!!, dbConfig["HLimitOrdersConnString"]!!, dbConfig["HLiquidityConnString"]!!)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(dbConfig["HMarketOrdersConnString"]!!, dbConfig["HTradesConnString"]!!)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(dbConfig["ClientPersonalInfoConnString"]!!, dbConfig["BitCoinQueueConnectionString"]!!, dbConfig["DictsConnString"]!!)
        this.azureQueueWriter = AzureQueueWriter(dbConfig["BitCoinQueueConnectionString"]!!)

        this.cashOperationService = CashOperationService(walletDatabaseAccessor, bitcoinQueue)
        this.limitOrderService = LimitOrderService(limitOrderDatabaseAccessor, cashOperationService)
        this.marketOrderService = MarketOrderService(marketOrderDatabaseAccessor, limitOrderService, cashOperationService, bitcoinQueue, tradesInfoQueue)
        this.limitOrderCancelService = LimitOrderCancelService(limitOrderService)
        this.balanceUpdateService = BalanceUpdateService(cashOperationService)
        this.tradesInfoService = TradesInfoService(tradesInfoQueue, limitOrderDatabaseAccessor)

        this.backendQueueProcessor = BackendQueueProcessor(backOfficeDatabaseAccessor, bitcoinQueue, azureQueueWriter)

        val bestPricesInterval = config.getProperty("best.prices.interval")!!.toLong()
        this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = bestPricesInterval) {
            limitOrderDatabaseAccessor.updateBestPrices(limitOrderService.buildMarketProfile())
            LOGGER.debug("Wrote market profile")
        }

        var time = LocalDateTime.now()
//        time = time.plusMinutes(1).minusSeconds((time.second - 5).toLong()).minusNanos(time.nano.toLong())
        val candleSaverInterval = config.getProperty("candle.saver.interval")!!.toLong()
        this.candlesBuilder = fixedRateTimer(name = "CandleBuilder", initialDelay = ((1000 - time.nano/1000000) + 1000 * (63 - time.second)).toLong(), period = candleSaverInterval) {
            tradesInfoService.saveCandles()
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
            when (message.type) {
            //MessageType.PING -> already processed by client handler
                MessageType.CASH_OPERATION -> {
                    cashOperationService.processMessage(message)
                }
                MessageType.LIMIT_ORDER -> {
                    limitOrderService.processMessage(message)
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
                else -> {
                    LOGGER.error("Unknown message type: ${message.type}")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Got error during message processing: ${exception.message}", exception)
        }
    }
}