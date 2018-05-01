package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

abstract class AbstractLimitOrdersCanceller<TAssetOrderBook : AbstractAssetOrderBook,
        out TCancelResult: AbstractLimitOrdersCancelResult<TAssetOrderBook>>(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                             private val assetsPairsHolder: AssetsPairsHolder,
                                                                             private val balancesHolder: BalancesHolder,
                                                                             private val genericLimitOrderService: AbstractGenericLimitOrderService<TAssetOrderBook>,
                                                                             private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                                                             private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                                                             private val date: Date) {

    private val ordersToRemove = HashMap<String, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
    protected val ordersToCancel = HashMap<AssetPair, MutableMap<Boolean, MutableList<NewLimitOrder>>>()

    private val assetOrderBooks = HashMap<String, TAssetOrderBook>()

    private val walletOperations = LinkedList<WalletOperation>()

    fun preProcess(orders: Collection<NewLimitOrder>): AbstractLimitOrdersCanceller<TAssetOrderBook, TCancelResult> {
        val ordersToCancel = HashMap<AssetPair, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
        val ordersToRemove = HashMap<String, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
        orders.forEach { order ->
            val assetPair = try {
                assetsPairsHolder.getAssetPair(order.assetPairId)
            } catch (e: Exception) {
                dictionariesDatabaseAccessor.loadAssetPair(order.assetPairId, true)
            }
            if (assetPair == null) {
                ordersToRemove.getOrPut(order.assetPairId) { HashMap() }.getOrPut(order.isBuySide()) { LinkedList() }.add(order)
            } else {
                ordersToCancel.getOrPut(assetPair) { HashMap() }.getOrPut(order.isBuySide()) { LinkedList() }.add(order)
            }
        }
        return preProcess(ordersToCancel, ordersToRemove)
    }

    fun preProcess(ordersToCancel: Map<AssetPair, Map<Boolean, Collection<NewLimitOrder>>>,
                   ordersToRemove: Map<String, Map<Boolean, Collection<NewLimitOrder>>>): AbstractLimitOrdersCanceller<TAssetOrderBook, TCancelResult> {
        ordersToRemove.forEach { assetPairId, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                this.ordersToRemove.getOrPut(assetPairId) { HashMap() }
                        .getOrPut(isBuy) { LinkedList() }
                        .addAll(orders)
            }
        }

        ordersToCancel.forEach { assetPair, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                this.ordersToCancel.getOrPut(assetPair) { HashMap() }
                        .getOrPut(isBuy) { LinkedList() }
                        .addAll(orders)
                walletOperations.addAll(calculateWalletOperations(orders, date, assetPair))
            }
        }

        return this
    }

    private fun calculateWalletOperations(orders: Collection<NewLimitOrder>, date: Date, assetPair: AssetPair): List<WalletOperation> {
        val walletOperations = LinkedList<WalletOperation>()

        orders.forEach { order ->
            val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)

            if (!isTrustedClientOrder) {
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = getOrderLimitVolume(order)
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

                if (reservedBalance > 0.0) {
                    walletOperations.add(
                            WalletOperation(UUID.randomUUID().toString(), null, order.clientId, limitAsset, date, 0.0,
                                    if (limitVolume > reservedBalance) -reservedBalance else -limitVolume)
                    )
                }
            }
        }
        return walletOperations
    }

    protected abstract fun getOrderLimitVolume(order: NewLimitOrder): Double

    open fun apply(): TCancelResult {
        val clientsLimitOrders = LinkedList<LimitOrderWithTrades>()
        val trustedClientsLimitOrders = LinkedList<LimitOrderWithTrades>()

        ordersToRemove.forEach { assetPairId, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                removeOrdersFromAssetOrderBookCopy(orders, assetPairId, isBuy)
            }
        }

        ordersToCancel.forEach { assetPair, sideOrders ->
            val assetPairId = assetPair.assetPairId
            sideOrders.forEach { isBuy, orders ->
                removeOrdersFromAssetOrderBookCopy(orders, assetPairId, isBuy)
                val ordersWithTrades = calculateOrdersWithTrades(orders)
                clientsLimitOrders.addAll(ordersWithTrades.clientsOrders)
                trustedClientsLimitOrders.addAll(ordersWithTrades.trustedClientsOrders)
            }
        }

        //return AbstractLimitOrdersCancelResult(walletOperations, clientsLimitOrders, trustedClientsLimitOrders, assetOrderBooks)
        return getCancelResult(walletOperations, clientsLimitOrders, trustedClientsLimitOrders, assetOrderBooks)
    }

    protected abstract fun getCancelResult(walletOperations: List<WalletOperation>,
                                           clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                           trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                           assetOrderBooks: Map<String, TAssetOrderBook>): TCancelResult

    /** Cancels orders, updates balances and sends notifications */
    open fun applyFull(operationId: String, messageId: String, operationType: String, validateBalances: Boolean) {
        val result = apply()

        val walletProcessor = balancesHolder.createWalletProcessor(null, validateBalances)
        walletProcessor.preProcess(result.walletOperations)

        ordersToRemove.forEach { assetPairId, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                removeOrdersAndUpdateOrderBook(orders, assetPairId, isBuy)
            }
        }

        ordersToCancel.forEach { assetPair, sideOrders ->
            val assetPairId = assetPair.assetPairId
            sideOrders.forEach { isBuy, orders ->
                removeOrdersAndUpdateOrderBook(orders, assetPairId, isBuy)
            }
        }

        if (result.clientsOrdersWithTrades.isNotEmpty()) {
            clientsLimitOrdersQueue.put(LimitOrdersReport(messageId, result.clientsOrdersWithTrades.toMutableList()))
        }
        if (result.trustedClientsOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, result.trustedClientsOrdersWithTrades.toMutableList()))
        }

        walletProcessor.apply(operationId, operationType, messageId)
    }

    @Suppress("unchecked_cast")
    private fun removeOrdersFromAssetOrderBookCopy(orders: Collection<NewLimitOrder>, assetPairId: String, isBuy: Boolean) {
        val assetOrderBook = assetOrderBooks.getOrPut(assetPairId) {
            genericLimitOrderService.getOrderBook(assetPairId).copy() as TAssetOrderBook
        }
        orders.forEach { order -> assetOrderBook.removeOrder(order) }
        processChangedOrderBook(assetOrderBook.copy() as TAssetOrderBook, isBuy)
    }

    protected abstract fun processChangedOrderBook(orderBookCopy: TAssetOrderBook, isBuy: Boolean)

    private fun removeOrdersAndUpdateOrderBook(orders: List<NewLimitOrder>, assetPairId: String, isBuy: Boolean) {
        genericLimitOrderService.cancelLimitOrders(orders)
        genericLimitOrderService.setOrderBook(assetPairId, assetOrderBooks[assetPairId]!!)
        genericLimitOrderService.updateOrderBook(assetPairId, isBuy)
    }

    private fun calculateOrdersWithTrades(orders: List<NewLimitOrder>): OrdersWithTrades {
        val trustedClientsOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val clientsOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        orders.forEach { order ->
            val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)
            if (isTrustedClientOrder && !order.isPartiallyMatched()) {
                trustedClientsOrdersWithTrades.add(LimitOrderWithTrades(order))
            } else {
                clientsOrdersWithTrades.add(LimitOrderWithTrades(order))
            }
        }
        return OrdersWithTrades(clientsOrdersWithTrades, trustedClientsOrdersWithTrades)
    }
}