package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.utils.plus
import org.springframework.stereotype.Component
import java.util.Date

@Component
class LimitOrdersCancelExecutorImpl(private val assetsPairsHolder: AssetsPairsHolder,
                                    private val executionContextFactory: ExecutionContextFactory,
                                    private val limitOrdersCanceller: LimitOrdersCanceller,
                                    private val stopOrderBookProcessor: StopOrderBookProcessor,
                                    private val executionDataApplyService: ExecutionDataApplyService) : LimitOrdersCancelExecutor {

    override fun cancelOrdersAndApply(request: CancelRequest): Boolean {
        with(request) {
            val executionContext = executionContextFactory.create(messageId,
                    requestId,
                    messageType,
                    processedMessage,
                    createAssetPairsByIdMapForOrders(plus(limitOrders, stopLimitOrders)),
                    Date(),
                    logger,
                    logger)

            executionContext.executionContextForCancelOperation = true

            limitOrdersCanceller.cancelOrders(limitOrders,
                    emptyList(),
                    stopLimitOrders,
                    emptyList(),
                    executionContext)

            if (cancelAll) {
                executionContext.currentTransactionMidPriceHolder.setRemoveAllFlag()
            }

            stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)

            return executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)
        }
    }

    private fun createAssetPairsByIdMapForOrders(orders: Collection<LimitOrder>): Map<String, AssetPair> {
        return orders.asSequence()
                .map { it.assetPairId }
                .toSet()
                .mapNotNull { assetsPairsHolder.getAssetPairAllowNulls(it) }
                .groupBy { it.assetPairId }
                .mapValues { it.value.single() }
    }
}