package com.lykke.matching.engine.messages

import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.LimitOrderCancelService
import com.lykke.matching.engine.services.LimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class MessageProcessor: Thread {

    companion object {
        val LOGGER = Logger.getLogger(MessageProcessor::class.java.name)
    }

    val messagesQueue: BlockingQueue<MessageWrapper>

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor
    val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor

    val cashOperationService: CashOperationService
    val limitOrderService: LimitOrderService
    val marketOrderService: MarketOrderService
    val limitOrderCancelService: LimitOrderCancelService

    constructor(config: Map<String, String>, queue: BlockingQueue<MessageWrapper>) {
        this.messagesQueue = queue
        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(config["BalancesInfoConnString"], config["DictsConnString"])
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config["ALimitOrdersConnString"], config["HLimitOrdersConnString"])
        this.marketOrderDatabaseAccessor = AzureMarketOrderDatabaseAccessor(config["HMarketOrdersConnString"], config["HTradesConnString"])

        this.cashOperationService = CashOperationService(walletDatabaseAccessor)
        this.limitOrderService = LimitOrderService(limitOrderDatabaseAccessor, cashOperationService)
        this.marketOrderService = MarketOrderService(marketOrderDatabaseAccessor, limitOrderService, cashOperationService)
        this.limitOrderCancelService = LimitOrderCancelService(limitOrderService)
    }

    override fun run() {
        while (true) {
            processMessage(messagesQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
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
    }
}