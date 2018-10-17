package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.OrderOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.math.BigDecimal
import java.util.*
import java.util.stream.Collectors
import kotlin.collections.ArrayList

abstract class AbstractLimitOrdersCanceller<TAssetOrderBook : AbstractAssetOrderBook,
        TCancelResult : AbstractLimitOrdersCancelResult<TAssetOrderBook>>(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                          private val assetsHolder: AssetsHolder,
                                                                          private val assetsPairsHolder: AssetsPairsHolder,
                                                                          private val balancesHolder: BalancesHolder,
                                                                          private val genericLimitOrderService: AbstractGenericLimitOrderService<TAssetOrderBook>,
                                                                          private val date: Date) {

    protected class OrdersProcessingInfo(buyOrders: List<LimitOrder>,
                                         sellOrders: List<LimitOrder>,
                                         val assetPairId: String) {
        val allOrders = ArrayList<LimitOrder>(buyOrders.size + sellOrders.size)

        init {
            allOrders.addAll(sellOrders)
            allOrders.addAll(buyOrders)
        }

    }

    private var allOrders: List<OrdersProcessingInfo> = LinkedList()
    private var ordersToCancel: List<OrdersProcessingInfo> = LinkedList()

    private val assetOrderBooks = HashMap<String, TAssetOrderBook>()

    private val changeBuyAssetPairsOrderBooks = HashSet<String>()
    private val changeSellAssetPairsOrderBooks = HashSet<String>()

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
                val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitAsset = assetsHolder.getAsset(limitAssetId)
                val limitVolume = getOrderLimitVolume(order, limitAsset)
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAssetId)

                if (reservedBalance > BigDecimal.ZERO) {
                    walletOperations.add(
                            WalletOperation(UUID.randomUUID().toString(), null, order.clientId, limitAssetId, date, BigDecimal.ZERO,
                                    if (limitVolume > reservedBalance) -reservedBalance else -limitVolume))
                }
            }
        }

        return walletOperations
    }

    fun getProcessedOrders(): List<LimitOrder> {
        return allOrders.flatMap { it.allOrders }
    }

    protected abstract fun getOrderLimitVolume(order: LimitOrder, limitAsset: Asset): BigDecimal

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
                    changedBuyOrderBooks.add(assetOrderBook)
                } else {
                    changedSellOrderBooks.add(assetOrderBook)
                }
            }
        }

        changeBuyAssetPairsOrderBooks.addAll(changedBuyOrderBooks.map { it.assetPairId })
        changeSellAssetPairsOrderBooks.addAll(changedSellOrderBooks.map { it.assetPairId })
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