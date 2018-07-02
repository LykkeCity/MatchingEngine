package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  assetsPairsHolder: AssetsPairsHolder,
                                  private val balancesHolder: BalancesHolder,
                                  orderBookQueue: BlockingQueue<OrderBook>,
                                  rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  genericLimitOrderService: GenericLimitOrderService,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                  date: Date,
                                  LOGGER: Logger) {

    private val limitOrdersCanceller = LimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsPairsHolder,
            balancesHolder,
            genericLimitOrderService,
            genericLimitOrderProcessorFactory,
            orderBookQueue,
            rabbitOrderBookQueue,
            clientLimitOrdersQueue,
            trustedClientsLimitOrdersQueue,
            date,
            LOGGER)

    private val stopLimitOrdersCanceller = StopLimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsPairsHolder,
            balancesHolder,
            genericStopLimitOrderService,
            clientLimitOrdersQueue,
            trustedClientsLimitOrdersQueue,
            date)

    fun preProcessLimitOrders(orders: Collection<LimitOrder>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessStopLimitOrders(orders: Collection<LimitOrder>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessLimitOrders(ordersToCancel: List<LimitOrder>,
                              ordersToRemove: List<LimitOrder>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun preProcessStopLimitOrders(ordersToCancel: List<LimitOrder>,
                                  ordersToRemove: List<LimitOrder>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun processLimitOrders(): LimitOrdersCancelResult {
        return limitOrdersCanceller.process()
    }

    fun processStopLimitOrders(): StopLimitOrdersCancelResult {
        return stopLimitOrdersCanceller.process()
    }

    fun applyFull(operationId: String, messageId: String, processedMessage: ProcessedMessage?, operationType: String, validateBalances: Boolean): Boolean {
        val limitOrdersCancelResult = processLimitOrders()
        val stopLimitOrdersResult = processStopLimitOrders()

        val walletProcessor = balancesHolder.createWalletProcessor(null, validateBalances)
        walletProcessor.preProcess(limitOrdersCancelResult.walletOperations)
        walletProcessor.preProcess(stopLimitOrdersResult.walletOperations)
        val updated = walletProcessor.persistBalances(processedMessage,
                limitOrdersCanceller.getPersistenceData(),
                stopLimitOrdersCanceller.getPersistenceData())
        if (!updated) {
            return false
        }

        walletProcessor.apply().sendNotification(operationId, operationType, messageId)
        stopLimitOrdersCanceller.apply(messageId, processedMessage, stopLimitOrdersResult)

        limitOrdersCanceller.apply(messageId, processedMessage, limitOrdersCancelResult)
        return true
    }
}