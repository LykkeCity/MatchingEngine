package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.stream.Collectors
import java.util.stream.Stream

abstract class AbstractLimitOrdersCanceller<TAssetOrderBook : AbstractAssetOrderBook,
        TCancelResult : AbstractLimitOrdersCancelResult<TAssetOrderBook>>(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                          private val assetsPairsHolder: AssetsPairsHolder,
                                                                          private val balancesHolder: BalancesHolder,
                                                                          private val genericLimitOrderService: AbstractGenericLimitOrderService<TAssetOrderBook>,
                                                                          private val clientLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                                                          private val trustedClientsLimitOrderQueue: BlockingQueue<JsonSerializable>,
                                                                          private val date: Date) {

    private var ordersToRemove: List<LimitOrder> = LinkedList()
    protected var ordersToCancel: List<LimitOrder> = LinkedList()

    private val assetOrderBooks = HashMap<String, TAssetOrderBook>()

    private val walletOperations = LinkedList<WalletOperation>()

    private enum class OrderOperation {
        CANCEL, REMOVE
    }

    fun preProcess(orders: Collection<LimitOrder>): AbstractLimitOrdersCanceller<TAssetOrderBook, TCancelResult> {
        val operationToOrders: Map<OrderOperation, List<LimitOrder>> = orders
                .stream()
                .collect(Collectors.groupingBy { getOrderOperation(it) })



        return preProcess(operationToOrders[OrderOperation.CANCEL] ?: emptyList(),
                operationToOrders[OrderOperation.REMOVE] ?: emptyList())
    }

    private fun getOrderOperation(order: LimitOrder): OrderOperation {
        assetsPairsHolder.getAssetPairAllowNulls(order.assetPairId) ?: return OrderOperation.REMOVE
        return OrderOperation.CANCEL

    }

    fun preProcess(ordersToCancel: List<LimitOrder>,
                   ordersToRemove: List<LimitOrder>): AbstractLimitOrdersCanceller<TAssetOrderBook, TCancelResult> {
        this.ordersToRemove = ordersToRemove
        this.ordersToCancel = ordersToCancel

        walletOperations.addAll(calculateWalletOperations(ordersToCancel, date))
        return this
    }

    private fun calculateWalletOperations(orders: Collection<LimitOrder>, date: Date): List<WalletOperation> {
        val walletOperations = LinkedList<WalletOperation>()

        orders.forEach { order ->
            val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)

            if (!isTrustedClientOrder) {
                val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = getOrderLimitVolume(order)
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

                if (reservedBalance > BigDecimal.ZERO) {
                    walletOperations.add(
                            WalletOperation(UUID.randomUUID().toString(), null, order.clientId, limitAsset, date, BigDecimal.ZERO,
                                    if (limitVolume > reservedBalance) -reservedBalance else -limitVolume)
                    )
                }
            }
        }
        return walletOperations
    }

    protected abstract fun getOrderLimitVolume(order: LimitOrder): BigDecimal

    fun process(): TCancelResult {
        val clientsLimitOrders = LinkedList<LimitOrderWithTrades>()
        val trustedClientsLimitOrders = LinkedList<LimitOrderWithTrades>()

        removeOrdersFromAssetOrderBookCopy(this.ordersToRemove)
        removeOrdersFromAssetOrderBookCopy(this.ordersToCancel)

        val ordersWithTrades = calculateOrdersWithTrades(ordersToCancel)
        clientsLimitOrders.addAll(ordersWithTrades.clientsOrders)
        trustedClientsLimitOrders.addAll(ordersWithTrades.trustedClientsOrders)

        return getCancelResult(walletOperations, clientsLimitOrders, trustedClientsLimitOrders, assetOrderBooks)
    }

    protected abstract fun getCancelResult(walletOperations: List<WalletOperation>,
                                           clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                           trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                           assetOrderBooks: Map<String, TAssetOrderBook>): TCancelResult

    open fun apply(messageId: String, result: TCancelResult) {
        removeOrdersAndUpdateOrderBooks()
        sendReports(messageId, result)
    }

    private fun removeOrdersAndUpdateOrderBooks() {
        val sideToOrder = Stream.concat(ordersToCancel.stream(), ordersToRemove.stream())
                .collect(Collectors.partitioningBy(LimitOrder::isBuySide))

        sideToOrder.forEach { buy, orders -> removeOrdersAndUpdateOrderBook(orders, buy) }
    }

    private fun sendReports(messageId: String, result: TCancelResult) {
        if (result.clientsOrdersWithTrades.isNotEmpty()) {
            clientLimitOrdersQueue.add(LimitOrdersReport(messageId, result.clientsOrdersWithTrades.toMutableList()))
        }
        if (result.trustedClientsOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrderQueue.add(LimitOrdersReport(messageId, result.trustedClientsOrdersWithTrades.toMutableList()))
        }
    }

    @Suppress("unchecked_cast")
    private fun removeOrdersFromAssetOrderBookCopy(orders: Collection<LimitOrder>) {
        val assetPairIdToOrders = orders
                .stream()
                .collect(Collectors.groupingBy(LimitOrder::assetPairId))

        val changedBuyOrderBooks = HashSet<TAssetOrderBook>()
        val changedSellOrderBooks = HashSet<TAssetOrderBook>()

        assetPairIdToOrders.forEach {assetPairId, ordersToProcess ->
            val assetOrderBook = assetOrderBooks.getOrPut(assetPairId) {
                genericLimitOrderService.getOrderBook(assetPairId).copy() as TAssetOrderBook
            }

            ordersToProcess.forEach {
                assetOrderBook.removeOrder(it)
                if (it.isBuySide()) {
                    changedBuyOrderBooks.add(assetOrderBook.copy() as TAssetOrderBook)
                } else {
                    changedSellOrderBooks.add(assetOrderBook.copy() as TAssetOrderBook)
                }
            }
        }

        changedBuyOrderBooks.forEach { processChangedOrderBook(it, true) }
        changedSellOrderBooks.forEach { processChangedOrderBook(it, false) }
    }

    protected abstract fun processChangedOrderBook(orderBookCopy: TAssetOrderBook, isBuy: Boolean)

    private fun removeOrdersAndUpdateOrderBook(orders: List<LimitOrder>, assetPairId: String, isBuy: Boolean) {
        genericLimitOrderService.cancelLimitOrders(orders, date)
        genericLimitOrderService.setOrderBook(assetPairId, assetOrderBooks[assetPairId]!!)
        genericLimitOrderService.updateOrderBook(assetPairId, isBuy)
    }

    private fun removeOrdersAndUpdateOrderBook(order: List<LimitOrder>, buy: Boolean) {
        val assetPairIdToLimitOrder: Map<String, List<LimitOrder>> = order
                .stream()
                .collect(Collectors.groupingBy { it.assetPairId })

        assetPairIdToLimitOrder.forEach { assetPairId, orders -> removeOrdersAndUpdateOrderBook(orders, assetPairId, buy) }
    }

    private fun calculateOrdersWithTrades(orders: List<LimitOrder>): OrdersWithTrades {
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