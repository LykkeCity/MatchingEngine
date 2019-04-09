package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.ClientAccountsHolder
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.utils.plus
import org.springframework.stereotype.Component
import java.util.Date
import java.util.stream.Collectors
import java.util.stream.Stream

@Component
class LimitOrdersCancelExecutorImpl(private val assetsPairsHolder: AssetsPairsHolder,
                                    private val clientAccountsHolder: ClientAccountsHolder,
                                    private val executionContextFactory: ExecutionContextFactory,
                                    private val limitOrdersCanceller: LimitOrdersCanceller,
                                    private val stopOrderBookProcessor: StopOrderBookProcessor,
                                    private val executionDataApplyService: ExecutionDataApplyService) : LimitOrdersCancelExecutor {

    override fun cancelOrdersAndApply(request: CancelRequest): Boolean {
        with(request) {
            val executionContext = executionContextFactory.create(messageId,
                    requestId,
                    messageType,
                    getWalletsByOperationWalletMap(request.limitOrders, request.stopLimitOrders),
                    processedMessage,
                    createAssetPairsByIdMapForOrders(plus(limitOrders, stopLimitOrders)),
                    Date(),
                    logger)

            limitOrdersCanceller.cancelOrders(limitOrders,
                    emptyList(),
                    stopLimitOrders,
                    emptyList(),
                    executionContext)

            stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)

            return executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)
        }
    }

    private fun getWalletsByOperationWalletMap(limitOrders: Collection<LimitOrder>, stopLimitOrder: Collection<LimitOrder>): Map<String, Set<String>> {
        val result = HashMap<String, Set<String>>()
        val allOperationWalletIds = Stream.concat(limitOrders.stream().map { limitOrder -> limitOrder.clientId },
                stopLimitOrder.stream().map { limitOrder -> limitOrder.clientId })
                .collect(Collectors.toSet())

        for (walletId in allOperationWalletIds) {
            result[walletId] = clientAccountsHolder.getAllWalletsByOperationWalletId(walletId)
        }

        return result
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