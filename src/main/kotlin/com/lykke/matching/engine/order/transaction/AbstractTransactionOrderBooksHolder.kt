package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date

abstract class AbstractTransactionOrderBooksHolder<AssetOrderBook : AbstractAssetOrderBook,
        GenericService : AbstractGenericLimitOrderService<AssetOrderBook>>(private val genericLimitOrderService: GenericService) {

    protected val newOrdersByExternalId = LinkedHashMap<String, LimitOrder>()
    protected val completedOrders = mutableListOf<LimitOrder>()
    protected val cancelledOrders = mutableListOf<LimitOrder>()
    protected val replacedOrders = mutableListOf<LimitOrder>()
    protected val assetOrderBookCopiesByAssetPairId = HashMap<String, AssetOrderBook>()
    protected val changedBuySides = HashSet<String>()
    protected val changedSellSides = HashSet<String>()

    open fun getChangedCopyOrOriginalOrderBook(assetPairId: String): AssetOrderBook {
        return assetOrderBookCopiesByAssetPairId[assetPairId] ?: genericLimitOrderService.getOrderBook(assetPairId)
    }

    @Suppress("unchecked_cast")
    fun getChangedOrderBookCopy(assetPairId: String): AssetOrderBook {
        return assetOrderBookCopiesByAssetPairId.getOrPut(assetPairId) {
            genericLimitOrderService.getOrderBook(assetPairId).copy() as AssetOrderBook
        }
    }

    fun setOrderBook(orderBook: AssetOrderBook) {
        assetOrderBookCopiesByAssetPairId[orderBook.assetPairId] = orderBook
    }

    fun addOrder(order: LimitOrder) {
        getChangedOrderBookCopy(order.assetPairId).addOrder(order)
        newOrdersByExternalId[order.externalId] = order
        addChangedSide(order)
    }

    fun addCompletedOrders(orders: Collection<LimitOrder>) {
        addRemovedOrders(orders, completedOrders)
    }

    fun addCancelledOrders(orders: Collection<LimitOrder>) {
        addRemovedOrders(orders, cancelledOrders)
    }

    fun addReplacedOrders(orders: Collection<LimitOrder>) {
        addRemovedOrders(orders, replacedOrders)
    }

    fun apply(date: Date, executionContext: ExecutionContext) {
        genericLimitOrderService.removeOrdersFromMapsAndSetStatus(completedOrders)
        genericLimitOrderService.removeOrdersFromMapsAndSetStatus(cancelledOrders, OrderStatus.Cancelled, date)
        genericLimitOrderService.removeOrdersFromMapsAndSetStatus(replacedOrders, OrderStatus.Replaced, date)
        genericLimitOrderService.addOrders(newOrdersByExternalId.values)
        applySpecificPart(date, executionContext)
    }

    protected abstract fun applySpecificPart(date: Date, executionContext: ExecutionContext)

    abstract fun getPersistenceData(): OrderBooksPersistenceData

    protected fun addRemovedOrders(orders: Collection<LimitOrder>, removedOrders: MutableCollection<LimitOrder>) {
        orders.forEach { order ->
            if (newOrdersByExternalId.containsKey(order.externalId)) {
                newOrdersByExternalId.remove(order.externalId)
            } else {
                addChangedSide(order)
                removedOrders.add(order)
            }
        }
    }

    protected fun addChangedSide(order: LimitOrder) {
        (if (order.isBuySide()) changedBuySides else changedSellSides).add(order.assetPairId)
    }
}