package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date

abstract class AbstractTransactionOrderBooksHolder<AssetOrderBook : AbstractAssetOrderBook,
        GenericService : AbstractGenericLimitOrderService<AssetOrderBook>>(protected val ordersService: GenericService): AbstractGenericLimitOrderService<AssetOrderBook> {

    protected val inputOrderCopyWrappers = mutableListOf<CopyWrapper<Order>>()
    protected val newOrdersByExternalId = LinkedHashMap<String, LimitOrder>()
    protected val removeOrdersByStatus =  HashMap<OrderStatus?, MutableList<LimitOrder>>()
    protected val assetOrderBookCopiesByAssetPairId = HashMap<String, AssetOrderBook>()

    protected val changedBuySides = HashSet<String>()
    protected val changedSellSides = HashSet<String>()

    override fun getOrderBook(assetPairId: String): AssetOrderBook {
        return assetOrderBookCopiesByAssetPairId[assetPairId] ?: ordersService.getOrderBook(assetPairId)
    }

    @Suppress("unchecked_cast")
    fun getChangedOrderBookCopy(assetPairId: String): AssetOrderBook {
        return assetOrderBookCopiesByAssetPairId.getOrPut(assetPairId) {
            ordersService.getOrderBook(assetPairId).copy() as AssetOrderBook
        }
    }

    override fun addOrders(orders: Collection<LimitOrder>) {
        orders.forEach { addOrder(it) }
    }

    fun addInputOrderCopyWrapper(orderCopyWrapper: CopyWrapper<Order>) {
        inputOrderCopyWrappers.add(orderCopyWrapper)
    }

    fun addOrder(order: LimitOrder) {
        getChangedOrderBookCopy(order.assetPairId).addOrder(order)
        newOrdersByExternalId[order.externalId] = order
        addChangedSide(order)
    }

    override fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus?, date: Date?) {
        val resultOrders = removeOrdersByStatus.getOrPut(status) {
            ArrayList()
        }

        orders.forEach { order ->
            if (newOrdersByExternalId.containsKey(order.externalId)) {
                newOrdersByExternalId.remove(order.externalId)
            } else {
                addChangedSide(order)
                resultOrders.add(order)
            }
        }
    }

    fun isOrderBookChanged(): Boolean {
        return !changedBuySides.isEmpty() || !changedSellSides.isEmpty()
    }

    fun apply(date: Date) {
        inputOrderCopyWrappers.forEach { orderCopyWrapper -> orderCopyWrapper.applyToOrigin()}

        removeOrdersByStatus.forEach { status, orders ->
            ordersService.removeOrdersFromMapsAndSetStatus(orders, status, date)
        }

        ordersService.addOrders(newOrdersByExternalId.values)
        applySpecificPart(date)
    }

    protected abstract fun applySpecificPart(date: Date)

    abstract fun getPersistenceData(): OrderBooksPersistenceData

    protected fun addChangedSide(order: LimitOrder) {
        (if (order.isBuySide()) changedBuySides else changedSellSides).add(order.assetPairId)
    }
}