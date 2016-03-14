package com.lykke.matching.engine.messages

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
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.LimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import org.apache.log4j.Logger
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

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor
    val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor

    val cashOperationService: CashOperationService
    val limitOrderService: LimitOrderService
    val marketOrderService: MarketOrderService
    val limitOrderCancelService: LimitOrderCancelService

    val backendQueueProcessor: BackendQueueProcessor
    val azureQueueWriter: QueueWriter

    val bestPriceBuilder: Timer

    constructor(config: Properties, dbConfig: Map<String, String>, queue: BlockingQueue<MessageWrapper>) {
        this.messagesQueue = queue
        this.bitcoinQueue = LinkedBlockingQueue<Transaction>()
        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(dbConfig["BalancesInfoConnString"]!!, dbConfig["DictsConnString"]!!)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(dbConfig["ALimitOrdersConnString"]!!, dbConfig["HLimitOrdersConnString"]!!, dbConfig["HLiquidityConnString"]!!)
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(dbConfig["HMarketOrdersConnString"]!!, dbConfig["HTradesConnString"]!!)
        this.backOfficeDatabaseAccessor = AzureBackOfficeDatabaseAccessor(dbConfig["ClientPersonalInfoConnString"]!!, dbConfig["BitCoinQueueConnectionString"]!!, dbConfig["DictsConnString"]!!)
        this.azureQueueWriter = AzureQueueWriter(dbConfig["BitCoinQueueConnectionString"]!!)

        this.cashOperationService = CashOperationService(walletDatabaseAccessor, bitcoinQueue)
        this.limitOrderService = LimitOrderService(limitOrderDatabaseAccessor, cashOperationService)
        this.marketOrderService = MarketOrderService(marketOrderDatabaseAccessor, limitOrderService, cashOperationService, bitcoinQueue)
        this.limitOrderCancelService = LimitOrderCancelService(limitOrderService)

        this.backendQueueProcessor = BackendQueueProcessor(backOfficeDatabaseAccessor, bitcoinQueue, azureQueueWriter)

        val bestPricesInterval = config.getProperty("best.prices.interval")!!.toLong()
        this.bestPriceBuilder = fixedRateTimer(name = "BestPriceBuilder", initialDelay = 0, period = bestPricesInterval) {
            limitOrderDatabaseAccessor.updateBestPrices(limitOrderService.buildMarketProfile())
            LOGGER.debug("Wrote market profile")
        }

    }

    override fun run() {
        backendQueueProcessor.start()
        while (true) {
            processMessage(messagesQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
        try {
            when (message.type) {
            //MessageType.PING -> already processed by client handler
                MessageType.UPDATE_BALANCE -> {
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
                else -> {
                    LOGGER.error("Unknown message type: ${message.type}")
                }
            }
        } catch (exception: Exception) {
            LOGGER.error("Got error during message processing: ${exception.message}", exception)
        }
    }
}