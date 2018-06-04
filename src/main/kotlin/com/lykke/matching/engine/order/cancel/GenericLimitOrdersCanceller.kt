package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  assetsPairsHolder: AssetsPairsHolder,
                                  private val balancesHolder: BalancesHolder,
                                  genericLimitOrderService: GenericLimitOrderService,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                  trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                  clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                  orderBookQueue: BlockingQueue<OrderBook>,
                                  rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                                  date: Date,
                                  LOGGER: Logger) {

    private val limitOrdersCanceller = LimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsPairsHolder,
            balancesHolder,
            genericLimitOrderService,
            genericLimitOrderProcessorFactory,
            trustedClientsLimitOrdersQueue,
            clientsLimitOrdersQueue,
            orderBookQueue,
            rabbitOrderBookQueue,
            date,
            LOGGER)

    private val stopLimitOrdersCanceller = StopLimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsPairsHolder,
            balancesHolder,
            genericStopLimitOrderService,
            trustedClientsLimitOrdersQueue,
            clientsLimitOrdersQueue,
            date)

    fun preProcessLimitOrders(orders: Collection<NewLimitOrder>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessStopLimitOrders(orders: Collection<NewLimitOrder>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessLimitOrders(ordersToCancel: Map<AssetPair, Map<Boolean, Collection<NewLimitOrder>>>,
                              ordersToRemove: Map<String, Map<Boolean, Collection<NewLimitOrder>>>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun preProcessStopLimitOrders(ordersToCancel: Map<AssetPair, Map<Boolean, Collection<NewLimitOrder>>>,
                                  ordersToRemove: Map<String, Map<Boolean, Collection<NewLimitOrder>>>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun processLimitOrders(): LimitOrdersCancelResult {
        return limitOrdersCanceller.process()
    }

    fun processStopLimitOrders(): StopLimitOrdersCancelResult {
        return stopLimitOrdersCanceller.process()
    }

    fun applyFull(operationId: String, messageId: String, operationType: String, validateBalances: Boolean): Boolean {
        val limitOrdersCancelResult = processLimitOrders()
        val stopLimitOrdersResult = processStopLimitOrders()

        val walletProcessor = balancesHolder.createWalletProcessor(null, validateBalances)
        walletProcessor.preProcess(limitOrdersCancelResult.walletOperations)
        walletProcessor.preProcess(stopLimitOrdersResult.walletOperations)
        val updated = walletProcessor.persistBalances()
        if (!updated) {
            return false
        }

        walletProcessor.apply().sendNotification(operationId, operationType, messageId)
        stopLimitOrdersCanceller.apply(messageId, stopLimitOrdersResult)

        limitOrdersCanceller.apply(messageId, limitOrdersCancelResult)
        return true
    }
}