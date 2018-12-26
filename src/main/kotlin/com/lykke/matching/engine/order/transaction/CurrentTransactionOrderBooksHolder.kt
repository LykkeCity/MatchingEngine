package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.matching.UpdatedOrderBookAndOrder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AbstractGenericLimitOrderService
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import java.math.BigDecimal
import java.util.Date
import java.util.HashMap
import java.util.concurrent.PriorityBlockingQueue

class CurrentTransactionOrderBooksHolder(ordersService: AbstractGenericLimitOrderService<AssetOrderBook>)
    : AbstractTransactionOrderBooksHolder<AssetOrderBook, AbstractGenericLimitOrderService<AssetOrderBook>>(ordersService) {

    private val orderCopyWrappersByOriginalOrder = HashMap<LimitOrder, CopyWrapper<LimitOrder>>()

    val tradeInfoList = mutableListOf<TradeInfo>()
    val outgoingOrderBooks = mutableListOf<OrderBook>()

    fun getOrPutOrderCopyWrapper(limitOrder: LimitOrder, defaultValue: () -> CopyWrapper<LimitOrder>): CopyWrapper<LimitOrder> {
        return orderCopyWrappersByOriginalOrder.getOrPut(limitOrder) {
            val copyWrapper = (if (ordersService is CurrentTransactionOrderBooksHolder) {
                ordersService.getOrderCopyWrapper(limitOrder)
            } else {
                null
            })?.let {
                CopyWrapper(it.origin, it.copy.copy())
            }

            addChangedSide(limitOrder)
            copyWrapper ?: defaultValue()
        }
    }

    fun getOrderCopyWrapper(limitOrder: LimitOrder): CopyWrapper<LimitOrder>? {
        return orderCopyWrappersByOriginalOrder[limitOrder]
    }

    fun getChangedOrderBooksMidPricesByAssetPairIdMap(): Map<String, BigDecimal> {
        val result = HashMap<String, BigDecimal>()

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            orderBook.getMidPrice()?.let {
                result[assetPairId] = it
            }
        }

        return result
    }

    override fun getPersistenceData(): OrderBooksPersistenceData {
        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSave = mutableListOf<LimitOrder>()
        val ordersToRemove = ArrayList(removeOrdersByStatus.values.flatMap { it })

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            if (changedBuySides.contains(assetPairId)) {
                val updatedOrders = createUpdatedOrderBookAndOrder(orderBook.getOrderBook(true))
                orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, true, updatedOrders.updatedOrderBook))
                updatedOrders.updatedOrder?.let { ordersToSave.add(it) }
            }
            if (changedSellSides.contains(assetPairId)) {
                val updatedOrders = createUpdatedOrderBookAndOrder(orderBook.getOrderBook(false))
                orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, false, updatedOrders.updatedOrderBook))
                updatedOrders.updatedOrder?.let { ordersToSave.add(it) }
            }
        }

        ordersToSave.addAll(newOrdersByExternalId.values)
        return OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove)
    }

    private fun createUpdatedOrderBookAndOrder(orderBook: PriorityBlockingQueue<LimitOrder>): UpdatedOrderBookAndOrder {
        val updatedOrderBook = ArrayList<LimitOrder>(orderBook)

        val bestOrder = orderBook.peek()
                ?: return UpdatedOrderBookAndOrder(updatedOrderBook, null)

        val updatedBestOrder = orderCopyWrappersByOriginalOrder[bestOrder]?.copy
                ?: return UpdatedOrderBookAndOrder(updatedOrderBook, null)

        updatedOrderBook.remove(bestOrder)
        updatedOrderBook.add(0, updatedBestOrder)
        return UpdatedOrderBookAndOrder(updatedOrderBook, updatedBestOrder)
    }

    override fun applySpecificPart(date: Date) {
        if (ordersService is CurrentTransactionOrderBooksHolder) {
            ordersService.orderCopyWrappersByOriginalOrder.putAll(this.orderCopyWrappersByOriginalOrder)
            this.orderCopyWrappersByOriginalOrder.keys.forEach {
                if (it.isBuySide()) {
                    changedBuySides.add(it.assetPairId)
                } else {
                    changedSellSides.add(it.assetPairId)
                }
            }
        }

        if (ordersService is GenericLimitOrderService) {
            orderCopyWrappersByOriginalOrder.forEach { it.value.applyToOrigin() }
        }

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            ordersService.setOrderBook(assetPairId, orderBook)
            if (ordersService is GenericLimitOrderService) {
                val orderBookCopy = orderBook.copy()
                if (changedBuySides.contains(assetPairId)) {
                    processChangedOrderBookSide(orderBookCopy, true, date)
                }
                if (changedSellSides.contains(assetPairId)) {
                    processChangedOrderBookSide(orderBookCopy, false, date)
                }
            }

        }
    }

    private fun processChangedOrderBookSide(orderBookCopy: AssetOrderBook,
                                            isBuySide: Boolean,
                                            date: Date) {
        val assetPairId = orderBookCopy.assetPairId
        val price = if (isBuySide) orderBookCopy.getBidPrice() else orderBookCopy.getAskPrice()
        tradeInfoList.add(TradeInfo(assetPairId, isBuySide, price, date))
        outgoingOrderBooks.add(OrderBook(assetPairId,
                isBuySide,
                date,
                orderBookCopy.getOrderBook(isBuySide)))
    }
}