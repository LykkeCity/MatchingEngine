package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetStopOrderBook
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import java.math.BigDecimal
import java.util.Date

class CurrentTransactionStopOrderBooksHolder(private val genericStopLimitOrderService: GenericStopLimitOrderService)
    : AbstractTransactionOrderBooksHolder<AssetStopOrderBook, GenericStopLimitOrderService>(genericStopLimitOrderService) {

    fun pollStopOrderToExecute(assetPairId: String,
                               bestBidPrice: BigDecimal,
                               bestAskPrice: BigDecimal,
                               date: Date): LimitOrder? {
        return pollStopOrderToExecute(assetPairId, bestBidPrice, false, date)
                ?: pollStopOrderToExecute(assetPairId, bestAskPrice, true, date)
    }

    private fun pollStopOrderToExecute(assetPairId: String,
                                       bestOppositePrice: BigDecimal,
                                       isBuySide: Boolean,
                                       date: Date): LimitOrder? {
        if (bestOppositePrice <= BigDecimal.ZERO) {
            return null
        }
        val stopOrderBook = getOrderBook(assetPairId)
        var order: LimitOrder?
        var orderPrice: BigDecimal? = null
        order = stopOrderBook.getOrder(bestOppositePrice, isBuySide, true)
        if (order != null) {
            orderPrice = order.lowerPrice!!
        } else {
            order = stopOrderBook.getOrder(bestOppositePrice, isBuySide, false)
            if (order != null) {
                orderPrice = order.upperPrice!!
            }
        }
        if (order == null) {
            return null
        }
        removeOrdersFromMapsAndSetStatus(listOf(order))
        getChangedOrderBookCopy(assetPairId).removeOrder(order)
        val orderCopy = order.copy()
        orderCopy.price = orderPrice!!
        orderCopy.updateStatus(OrderStatus.InOrderBook, date)
        return orderCopy
    }

    override fun applySpecificPart(date: Date) {
        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            genericStopLimitOrderService.setOrderBook(assetPairId, orderBook)
        }
    }

    override fun getPersistenceData(): OrderBooksPersistenceData {
        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSave = mutableListOf<LimitOrder>()
        val ordersToRemove = ArrayList<LimitOrder>(removeOrdersByStatus.values.flatMap { it })

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            if (changedBuySides.contains(assetPairId)) {
                orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, true, orderBook.getOrderBook(true)))
            }
            if (changedSellSides.contains(assetPairId)) {
                orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, false, orderBook.getOrderBook(false)))
            }
        }

        ordersToSave.addAll(newOrdersByExternalId.values)
        return OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove)
    }

}