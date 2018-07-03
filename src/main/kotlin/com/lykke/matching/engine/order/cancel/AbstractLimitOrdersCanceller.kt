package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.OrderOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.stream.Collectors

abstract class AbstractLimitOrdersCanceller<TAssetOrderBook : AbstractAssetOrderBook,
        TCancelResult : AbstractLimitOrdersCancelResult<TAssetOrderBook>>(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                          private val assetsPairsHolder: AssetsPairsHolder,
                                                                          private val balancesHolder: BalancesHolder,
                                                                          private val genericLimitOrderService: AbstractGenericLimitOrderService<TAssetOrderBook>,
                                                                          private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                                          private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                                          private val date: Date) {

    protected class OrdersProcessingInfo {
        val assetPairId: String
        val allOrders = LinkedList<LimitOrder>()
        val buyOrders: List<LimitOrder>
        val sellOrders: List<LimitOrder>

        constructor(buyOrders: List<LimitOrder>, sellOrders: List<LimitOrder>, assetPairId: String) {
            this.assetPairId = assetPairId
            allOrders.addAll(sellOrders)
            allOrders.addAll(buyOrders)
            this.buyOrders = buyOrders
            this.sellOrders = sellOrders
        }
    }

    private var allOrders: List<OrdersProcessingInfo> = LinkedList()
    protected var ordersToCancel: List<OrdersProcessingInfo> = LinkedList()

    private val assetOrderBooks = HashMap<String, TAssetOrderBook>()

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
        this.ordersToCancel = convertOrdersToOrderProcessingInfo(ordersToCancel)

        val allOrders = LinkedList<LimitOrder>()

        allOrders.addAll(ordersToCancel)
        allOrders.addAll(ordersToRemove)

        this.allOrders = convertOrdersToOrderProcessingInfo(allOrders)

        return this
    }

    private fun calculateWalletOperations(ordersInfo: Collection<OrdersProcessingInfo>, date: Date): List<WalletOperation> {
        val walletOperations = LinkedList<WalletOperation>()

        ordersInfo.forEach { orderInfo ->
            val assetPair = assetsPairsHolder.getAssetPairAllowNulls(orderInfo.assetPairId)
            if (assetPair != null) {
                walletOperations.addAll(calculateWalletOperation(orderInfo, assetPair, date))
            }
        }
        return walletOperations
    }

    private fun calculateWalletOperation(orderInfo: OrdersProcessingInfo, assetPair: AssetPair, date: Date): List<WalletOperation> {
        val walletOperations = LinkedList<WalletOperation>()

        orderInfo.allOrders.forEach { order ->
            val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)

            if (!isTrustedClientOrder) {
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = getOrderLimitVolume(order)
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

                if (reservedBalance > BigDecimal.ZERO) {
                    walletOperations.add(
                            WalletOperation(UUID.randomUUID().toString(), null, order.clientId, limitAsset, date, BigDecimal.ZERO,
                                    if (limitVolume > reservedBalance) -reservedBalance else -limitVolume))
                }
            }
        }

        return walletOperations
    }

    protected abstract fun getOrderLimitVolume(order: LimitOrder): BigDecimal

    fun process(): TCancelResult {
        val clientsLimitOrders = LinkedList<LimitOrderWithTrades>()
        val trustedClientsLimitOrders = LinkedList<LimitOrderWithTrades>()

        removeOrdersFromAssetOrderBookCopy(this.allOrders)

        val ordersWithTrades = calculateOrdersWithTrades(ordersToCancel)
        clientsLimitOrders.addAll(ordersWithTrades.clientsOrders)
        trustedClientsLimitOrders.addAll(ordersWithTrades.trustedClientsOrders)

        return getCancelResult(calculateWalletOperations(this.ordersToCancel, date), clientsLimitOrders, trustedClientsLimitOrders, assetOrderBooks)
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
        allOrders
                .forEach {
                    removeOrdersAndUpdateOrderBook(it.buyOrders, it.assetPairId, true)
                    removeOrdersAndUpdateOrderBook(it.sellOrders, it.assetPairId, false)
                }
    }

    private fun sendReports(messageId: String, result: TCancelResult) {
        if (result.clientsOrdersWithTrades.isNotEmpty()) {
            clientLimitOrdersQueue.put(LimitOrdersReport(messageId, result.clientsOrdersWithTrades.toMutableList()))
        }
        if (result.trustedClientsOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, result.trustedClientsOrdersWithTrades.toMutableList()))
        }
    }

    @Suppress("unchecked_cast")
    private fun removeOrdersFromAssetOrderBookCopy(orders: List<OrdersProcessingInfo>) {
        val changedBuyOrderBooks = HashSet<TAssetOrderBook>()
        val changedSellOrderBooks = HashSet<TAssetOrderBook>()

        orders.forEach { ordersInfo ->
            val assetOrderBook = assetOrderBooks.getOrPut(ordersInfo.assetPairId) {
                genericLimitOrderService.getOrderBook(ordersInfo.assetPairId).copy() as TAssetOrderBook
            }

            ordersInfo.allOrders.forEach {
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

    private fun calculateOrdersWithTrades(orders: List<OrdersProcessingInfo>): OrdersWithTrades {
        val trustedClientsOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val clientsOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        orders
                .stream()
                .flatMap { it.allOrders.stream() }
                .forEach { order ->
                    val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)
                    if (isTrustedClientOrder && !order.isPartiallyMatched()) {
                        trustedClientsOrdersWithTrades.add(LimitOrderWithTrades(order))
                    } else {
                        clientsOrdersWithTrades.add(LimitOrderWithTrades(order))
                    }
                }
        return OrdersWithTrades(clientsOrdersWithTrades, trustedClientsOrdersWithTrades)
    }

    private fun convertOrdersToOrderProcessingInfo(order: List<LimitOrder>): List<OrdersProcessingInfo> {
        return order
                .stream()
                .collect(Collectors.groupingBy(LimitOrder::assetPairId))
                .entries
                .stream()
                .map {
                    val sideToOrder = it
                            .value
                            .stream()
                            .collect(Collectors.groupingBy(LimitOrder::isBuySide))
                    OrdersProcessingInfo(sideToOrder[true] ?: emptyList(),
                            sideToOrder[false] ?: emptyList(),
                            it.key)
                }.collect(Collectors.toList())
    }
}