package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.context.PreviousLimitOrdersContext
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component

@Component
class PreviousLimitOrdersProcessor(private val genericLimitOrderService: GenericLimitOrderService,
                                   private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                   private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory) {
    companion object {
        class Result(val ordersWithNotFoundPrevious: Collection<LimitOrder>,
                     val ordersToReplace: Collection<LimitOrder>,
                     val ordersToCancelWithoutReplacements: Collection<LimitOrder>,
                     val stopOrdersToReplace: Collection<LimitOrder>,
                     val stopOrdersToCancelWithoutReplacements: Collection<LimitOrder>)

        private fun defaultResult(ordersToCancel: Collection<LimitOrder>?, stopOrdersToCancel: Collection<LimitOrder>?) =
                Result(emptyList(),
                        emptyList(),
                        ordersToCancel ?: emptyList(),
                        emptyList(),
                        stopOrdersToCancel ?: emptyList())
    }

    fun cancelAndReplaceOrders(clientId: String,
                               assetPairId: String,
                               cancelAllPreviousLimitOrders: Boolean,
                               cancelBuySide: Boolean,
                               cancelSellSide: Boolean,
                               buyReplacementsByPreviousExternalId: Map<String, LimitOrder>,
                               sellReplacementsByPreviousExternalId: Map<String, LimitOrder>,
                               executionContext: ExecutionContext) {
        val context = PreviousLimitOrdersContext(clientId,
                assetPairId,
                cancelAllPreviousLimitOrders,
                cancelBuySide,
                cancelSellSide,
                buyReplacementsByPreviousExternalId,
                sellReplacementsByPreviousExternalId,
                executionContext)

        if (context.cancelAllPreviousLimitOrders) {
            if (context.cancelBuySide) {
                context.buyOrdersToCancel = genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, true)
                context.buyStopOrdersToCancel = genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, true)
            }
            if (context.cancelSellSide) {
                context.sellOrdersToCancel = genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, false)
                context.sellStopOrdersToCancel = genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, false)
            }
        }

        val replacementsResult = splitOrdersIntoCancelledAndReplaced(context)

        val ordersToCancel = replacementsResult.ordersToCancelWithoutReplacements
        val ordersToReplace = replacementsResult.ordersToReplace
        val stopOrdersToCancel = replacementsResult.stopOrdersToCancelWithoutReplacements
        val stopOrdersToReplace = replacementsResult.stopOrdersToReplace

        val canceller = genericLimitOrdersCancellerFactory.create(executionContext.logger, executionContext.date)
                .preProcessLimitOrders(ordersToCancel.plus(ordersToReplace))
                .preProcessStopLimitOrders(stopOrdersToCancel.plus(stopOrdersToReplace))

        val cancelResult = canceller.processLimitOrders()
        val stopOrdersCancelResult = canceller.processStopLimitOrders()

        replacementsResult.ordersWithNotFoundPrevious.forEach {
            it.updateStatus(OrderStatus.NotFoundPrevious, executionContext.date)
        }

        executionContext.walletOperationsProcessor.preProcess(cancelResult.walletOperations.plus(stopOrdersCancelResult.walletOperations), true)
        cancelResult.assetOrderBooks[context.assetPairId]?.let { executionContext.orderBooksHolder.setOrderBook(it) }
        stopOrdersCancelResult.assetOrderBooks[context.assetPairId]?.let { executionContext.stopOrderBooksHolder.setOrderBook(it) }
        executionContext.orderBooksHolder.addCancelledOrders(ordersToCancel)
        executionContext.orderBooksHolder.addReplacedOrders(ordersToReplace)
        executionContext.stopOrderBooksHolder.addCancelledOrders(stopOrdersToCancel)
        executionContext.stopOrderBooksHolder.addReplacedOrders(stopOrdersToReplace)
        executionContext.addClientsLimitOrdersWithTrades(cancelResult.clientsOrdersWithTrades
                .plus(stopOrdersCancelResult.clientsOrdersWithTrades))
        executionContext.addTrustedClientsLimitOrdersWithTrades(cancelResult.trustedClientsOrdersWithTrades
                .plus(stopOrdersCancelResult.trustedClientsOrdersWithTrades))
    }

    private fun splitOrdersIntoCancelledAndReplaced(context: PreviousLimitOrdersContext): Result {
        val buySideResult = splitOneSideLimitAndStopOrders(context,
                context.buyReplacementsByPreviousExternalId,
                true,
                context.buyOrdersToCancel,
                context.buyStopOrdersToCancel)
        val sellSideResult = splitOneSideLimitAndStopOrders(context,
                context.sellReplacementsByPreviousExternalId,
                false,
                context.sellOrdersToCancel,
                context.sellStopOrdersToCancel)

        return Result(buySideResult.ordersWithNotFoundPrevious.plus(sellSideResult.ordersWithNotFoundPrevious),
                buySideResult.ordersToReplace.plus(sellSideResult.ordersToReplace),
                buySideResult.ordersToCancelWithoutReplacements.plus(sellSideResult.ordersToCancelWithoutReplacements),
                buySideResult.stopOrdersToReplace.plus(sellSideResult.stopOrdersToReplace),
                buySideResult.stopOrdersToCancelWithoutReplacements.plus(sellSideResult.stopOrdersToCancelWithoutReplacements))
    }

    private fun splitOneSideLimitAndStopOrders(context: PreviousLimitOrdersContext,
                                               replacementsByPreviousExternalId: Map<String, LimitOrder>,
                                               isBuy: Boolean,
                                               ordersToCancel: Collection<LimitOrder>?,
                                               stopOrdersToCancel: Collection<LimitOrder>?): Result {
        if (replacementsByPreviousExternalId.isEmpty()) {
            return defaultResult(ordersToCancel, stopOrdersToCancel)
        }
        val isAlreadyCancelled = ordersToCancel != null
        val ordersToReplace = mutableListOf<LimitOrder>()
        val ordersToCancelWithoutReplacements = (if (isAlreadyCancelled) ordersToCancel!! else emptyList()).toMutableList()
        val stopOrdersToReplace = mutableListOf<LimitOrder>()
        val stopOrdersToCancelWithoutReplacements = (if (isAlreadyCancelled) stopOrdersToCancel!! else emptyList()).toMutableList()
        val ordersWithNotFoundPreviousByPreviousExternalId = replacementsByPreviousExternalId.toMutableMap()

        val ordersToCheck = ordersToCancel
                ?: genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, isBuy)
        splitOneSideOrders(context,
                isAlreadyCancelled,
                replacementsByPreviousExternalId,
                ordersToCheck,
                ordersWithNotFoundPreviousByPreviousExternalId,
                ordersToReplace,
                ordersToCancelWithoutReplacements)

        val stopOrdersToCheck = stopOrdersToCancel
                ?: genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, isBuy)
        splitOneSideOrders(context,
                isAlreadyCancelled,
                replacementsByPreviousExternalId,
                stopOrdersToCheck,
                ordersWithNotFoundPreviousByPreviousExternalId,
                stopOrdersToReplace,
                stopOrdersToCancelWithoutReplacements)

        return Result(ordersWithNotFoundPreviousByPreviousExternalId.values,
                ordersToReplace,
                ordersToCancelWithoutReplacements,
                stopOrdersToReplace,
                stopOrdersToCancelWithoutReplacements)
    }

    private fun splitOneSideOrders(context: PreviousLimitOrdersContext,
                                   isAlreadyCancelled: Boolean,
                                   replacementsByPreviousExternalId: Map<String, LimitOrder>,
                                   ordersToCheck: Collection<LimitOrder>,
                                   ordersWithNotFoundPreviousByPreviousExternalId: MutableMap<String, LimitOrder>,
                                   ordersToReplace: MutableCollection<LimitOrder>,
                                   ordersToCancelWithoutReplacements: MutableCollection<LimitOrder>) {
        ordersToCheck.forEach {
            if (!ordersWithNotFoundPreviousByPreviousExternalId.containsKey(it.externalId)) {
                return@forEach
            }
            val newOrder = ordersWithNotFoundPreviousByPreviousExternalId.remove(it.externalId)
            if (newOrder != null && isOrderStatusValid(newOrder)) {
                context.executionContext.info("Order ${it.externalId} is replaced by ${replacementsByPreviousExternalId[it.externalId]?.externalId}")
                if (isAlreadyCancelled) {
                    ordersToCancelWithoutReplacements.remove(it)
                }
                ordersToReplace.add(it)
            } else if (!isAlreadyCancelled) {
                ordersToCancelWithoutReplacements.add(it)
            }
        }
    }

    private fun isOrderStatusValid(order: LimitOrder): Boolean {
        return order.status == OrderStatus.InOrderBook.name || order.status == OrderStatus.Pending.name
    }
}