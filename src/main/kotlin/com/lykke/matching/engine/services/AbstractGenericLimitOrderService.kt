package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date

abstract class AbstractGenericLimitOrderService<T : AbstractAssetOrderBook> {

    abstract fun getOrderBook(assetPairId: String): T
    abstract fun cancelLimitOrders(orders: Collection<LimitOrder>, date: Date)
    abstract fun setOrderBook(assetPairId: String, assetOrderBook: T)
    abstract fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus? = null, date: Date? = null)
    abstract fun addOrders(orders: Collection<LimitOrder>)

    fun searchOrders(clientId: String?, assetPairId: String?, isBuy: Boolean?): List<LimitOrder> {
        return when {
            clientId != null -> searchClientOrders(clientId, assetPairId, isBuy)
            assetPairId != null -> searchAssetPairOrders(assetPairId, isBuy)
            else -> getOrderBooksByAssetPairIdMap().keys.flatMap { searchAssetPairOrders(it, isBuy) }
        }
    }

    private fun searchClientOrders(clientId: String, assetPairId: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = mutableListOf<LimitOrder>()
        getLimitOrdersByClientIdMap()[clientId]?.forEach { limitOrder ->
            if ((assetPairId == null || limitOrder.assetPairId == assetPairId) && (isBuy == null || limitOrder.isBuySide() == isBuy)) {
                result.add(limitOrder)
            }
        }
        return result
    }

    private fun searchAssetPairOrders(assetPairId: String, isBuy: Boolean?): List<LimitOrder> {
        val orderBook = getOrderBooksByAssetPairIdMap()[assetPairId] ?: return emptyList()
        val result = mutableListOf<LimitOrder>()
        if (isBuy == null || isBuy) {
            result.addAll(orderBook.getBuyOrderBook())
        }
        if (isBuy == null || !isBuy) {
            result.addAll(orderBook.getSellOrderBook())
        }
        return result
    }

    protected abstract fun getLimitOrdersByClientIdMap(): Map<String, Collection<LimitOrder>>
    protected abstract fun getOrderBooksByAssetPairIdMap(): Map<String, T>
}