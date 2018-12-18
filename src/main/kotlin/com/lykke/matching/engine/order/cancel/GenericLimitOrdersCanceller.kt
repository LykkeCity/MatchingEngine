package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.order.ExecutionDataApplyService
import org.apache.log4j.Logger
import java.util.Date

class GenericLimitOrdersCanceller(private val executionContextFactory: ExecutionContextFactory,
                                  private val stopOrderBookProcessor: StopOrderBookProcessor,
                                  private val executionDataApplyService: ExecutionDataApplyService,
                                  dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  assetsHolder: AssetsHolder,
                                  private val assetsPairsHolder: AssetsPairsHolder,
                                  balancesHolder: BalancesHolder,
                                  private val walletOperationsProcessorFactory: WalletOperationsProcessorFactory,
                                  genericLimitOrderService: GenericLimitOrderService,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  private val date: Date,
                                  private val LOGGER: Logger) {

    private val limitOrdersCanceller = LimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            genericLimitOrderService,
            date)

    private val stopLimitOrdersCanceller = StopLimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            genericStopLimitOrderService,
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

    fun applyFull(messageWrapper: MessageWrapper?,
                  operationId: String,
                  messageId: String,
                  processedMessage: ProcessedMessage?,
                  messageType: MessageType,
                  validateBalances: Boolean): Boolean {

        val limitOrdersCancelResult = processLimitOrders()
        val stopLimitOrdersResult = processStopLimitOrders()

        val assetPairsById = getAssetPairsByIdMap(limitOrdersCancelResult.assetOrderBooks.keys)
        val executionContext = executionContextFactory.create(messageId,
                operationId,
                messageType,
                processedMessage,
                assetPairsById,
                date,
                LOGGER,
                walletOperationsProcessor = walletOperationsProcessorFactory.create(LOGGER, validateBalances))

        executionContext.walletOperationsProcessor.preProcess(limitOrdersCancelResult.walletOperations
                .plus(stopLimitOrdersResult.walletOperations))

        limitOrdersCancelResult.assetOrderBooks.forEach {
            executionContext.orderBooksHolder.setOrderBook(it.value)
        }
        stopLimitOrdersResult.assetOrderBooks.forEach {
            executionContext.stopOrderBooksHolder.setOrderBook(it.value)
        }
        executionContext.orderBooksHolder.addCancelledOrders(limitOrdersCanceller.getProcessedOrders())
        executionContext.stopOrderBooksHolder.addCancelledOrders(stopLimitOrdersCanceller.getProcessedOrders())

        executionContext.addClientsLimitOrdersWithTrades(limitOrdersCancelResult.clientsOrdersWithTrades)
        executionContext.addClientsLimitOrdersWithTrades(stopLimitOrdersResult.clientsOrdersWithTrades)

        executionContext.addTrustedClientsLimitOrdersWithTrades(limitOrdersCancelResult.trustedClientsOrdersWithTrades)
        executionContext.addTrustedClientsLimitOrdersWithTrades(stopLimitOrdersResult.trustedClientsOrdersWithTrades)

        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)

        return executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)
    }

    private fun getAssetPairsByIdMap(assetPairIds: Collection<String>): Map<String, AssetPair> {
        return assetPairIds.asSequence()
                .mapNotNull { assetsPairsHolder.getAssetPairAllowNulls(it) }
                .groupBy { it.assetPairId }
                .mapValues { it.value.single() }
    }
}