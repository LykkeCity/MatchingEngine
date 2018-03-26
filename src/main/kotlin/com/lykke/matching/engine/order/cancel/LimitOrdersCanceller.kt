package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class LimitOrdersCanceller(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                           private val assetsPairsHolder: AssetsPairsHolder,
                           private val balancesHolder: BalancesHolder,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                           private val date: Date) {

    private val ordersToRemove = HashMap<String, MutableMap<Boolean, MutableList<NewLimitOrder>>>()
    private val ordersToCancel = HashMap<AssetPair, MutableMap<Boolean, MutableList<NewLimitOrder>>>()

    private val assetOrderBooks = HashMap<String, AssetOrderBook>()
    private val walletOperations = LinkedList<WalletOperation>()

    fun preProcess(orders: Collection<NewLimitOrder>): LimitOrdersCanceller {
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
                   ordersToRemove: Map<String, Map<Boolean, Collection<NewLimitOrder>>>): LimitOrdersCanceller {
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

    fun getTransitionalResult(): TransitionalLimitOrdersCancelResult = TransitionalLimitOrdersCancelResult(walletOperations.toList())

    fun apply(): LimitOrdersCancelResult {
        val clientsLimitOrders = LinkedList<LimitOrderWithTrades>()
        val trustedClientsLimitOrders = LinkedList<LimitOrderWithTrades>()
        val orderBooks = HashMap<String, OrderBook>()

        ordersToRemove.forEach { assetPairId, sideOrders ->
            sideOrders.forEach { isBuy, orders ->
                removeOrdersFromAssetOrderBookCopy(orders, assetPairId, isBuy, orderBooks)
            }
        }

        ordersToCancel.forEach { assetPair, sideOrders ->
            val assetPairId = assetPair.assetPairId
            sideOrders.forEach { isBuy, orders ->
                removeOrdersFromAssetOrderBookCopy(orders, assetPairId, isBuy, orderBooks)
                val ordersWithTrades = calculateOrdersWithTrades(orders)
                clientsLimitOrders.addAll(ordersWithTrades.clientsOrders)
                trustedClientsLimitOrders.addAll(ordersWithTrades.trustedClientsOrders)
            }
        }

        return LimitOrdersCancelResult(walletOperations, clientsLimitOrders, trustedClientsLimitOrders, assetOrderBooks, orderBooks.values.toList())
    }

    /** Cancels orders, updates balances and sends notifications */
    fun applyFull(operationId: String, operationType: String, validateBalances: Boolean = true) {
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

        result.orderBooks.forEach {orderBook ->
            genericLimitOrderService.putTradeInfo(TradeInfo(orderBook.assetPair, orderBook.isBuy, orderBook.prices.firstOrNull()?.price ?: 0.0, date))
            orderBookQueue.put(orderBook)
            rabbitOrderBookQueue.put(orderBook)
        }

        if (result.clientsOrdersWithTrades.isNotEmpty()) {
            clientsLimitOrdersQueue.put(LimitOrdersReport(result.clientsOrdersWithTrades.toMutableList()))
        }
        if (result.trustedClientsOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(result.trustedClientsOrdersWithTrades.toMutableList()))
        }

        walletProcessor.apply(operationId, operationType)
    }

    private fun removeOrdersFromAssetOrderBookCopy(orders: Collection<NewLimitOrder>, assetPairId: String, isBuy: Boolean, orderBooks: MutableMap<String, OrderBook>) {
        val assetOrderBook = assetOrderBooks.getOrPut(assetPairId) {
            genericLimitOrderService.getOrderBook(assetPairId).copy()
        }
        orders.forEach { order -> assetOrderBook.removeOrder(order) }
        orderBooks["$assetPairId|$isBuy"] = OrderBook(assetPairId, isBuy, date, assetOrderBook.copy().getOrderBook(isBuy))
    }

    private fun removeOrdersAndUpdateOrderBook(orders: List<NewLimitOrder>, assetPairId: String, isBuy: Boolean) {
        genericLimitOrderService.cancelLimitOrders(orders)
        genericLimitOrderService.setOrderBook(assetPairId, assetOrderBooks[assetPairId]!!)
        genericLimitOrderService.updateOrderBook(assetPairId, isBuy)
    }

    private fun calculateWalletOperations(orders: Collection<NewLimitOrder>, date: Date, assetPair: AssetPair): List<WalletOperation> {
        val walletOperations = LinkedList<WalletOperation>()

        orders.forEach { order ->
            val isTrustedClientOrder = balancesHolder.isTrustedClient(order.clientId)

            if (!isTrustedClientOrder) {
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = order.reservedLimitVolume
                        ?: if (order.isBuySide()) order.getAbsRemainingVolume() * order.price else order.getAbsRemainingVolume()
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

    private data class OrdersWithTrades(val clientsOrders: List<LimitOrderWithTrades>,
                                        val trustedClientsOrders: List<LimitOrderWithTrades>)

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