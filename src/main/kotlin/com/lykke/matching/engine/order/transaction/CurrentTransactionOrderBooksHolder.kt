package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.matching.UpdatedOrderBookAndOrder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import java.util.Date
import java.util.HashMap
import java.util.concurrent.PriorityBlockingQueue

open class CurrentTransactionOrderBooksHolder(private val genericLimitOrderService: GenericLimitOrderService)
    : AbstractTransactionOrderBooksHolder<AssetOrderBook, GenericLimitOrderService>(genericLimitOrderService) {

    private val orderCopyWrappersByOriginalOrder = HashMap<LimitOrder, CopyWrapper<LimitOrder>>()

    val tradeInfoList = mutableListOf<TradeInfo>()
    val outgoingOrderBooks = mutableListOf<OrderBook>()

    fun getOrPutOrderCopyWrapper(limitOrder: LimitOrder, defaultValue: () -> CopyWrapper<LimitOrder>): CopyWrapper<LimitOrder> {
        return orderCopyWrappersByOriginalOrder.getOrPut(limitOrder) {
            addChangedSide(limitOrder)
            defaultValue()
        }
    }

    override fun getPersistenceData(): OrderBooksPersistenceData {
        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSave = mutableListOf<LimitOrder>()
        val ordersToRemove = ArrayList<LimitOrder>(completedOrders.size + cancelledOrders.size + replacedOrders.size)

        ordersToRemove.addAll(completedOrders)
        ordersToRemove.addAll(cancelledOrders)
        ordersToRemove.addAll(replacedOrders)

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

    override fun applySpecificPart(date: Date, currentTransactionMidPriceHolder: CurrentTransactionMidPriceHolder, executionContext: ExecutionContext) {
        orderCopyWrappersByOriginalOrder.forEach { it.value.applyToOrigin() }
        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            genericLimitOrderService.setOrderBook(assetPairId, orderBook)
            val orderBookCopy = orderBook.copy()
            if (changedBuySides.contains(assetPairId)) {
                processChangedOrderBookSide(orderBookCopy, true, date, currentTransactionMidPriceHolder, executionContext)
            }
            if (changedSellSides.contains(assetPairId)) {
                processChangedOrderBookSide(orderBookCopy, false, date, currentTransactionMidPriceHolder, executionContext)
            }
        }
    }

    private fun processChangedOrderBookSide(orderBookCopy: AssetOrderBook,
                                            isBuySide: Boolean,
                                            date: Date,
                                            currentTransactionMidPriceHolder: CurrentTransactionMidPriceHolder,
                                            executionContext: ExecutionContext) {
        val assetPairId = orderBookCopy.assetPairId
        val price = if (isBuySide) orderBookCopy.getBidPrice() else orderBookCopy.getAskPrice()
        tradeInfoList.add(TradeInfo(assetPairId, isBuySide, price, date))
        outgoingOrderBooks.add(OrderBook(assetPairId,
                currentTransactionMidPriceHolder.getRefMidPrice(assetPairId, executionContext),
                currentTransactionMidPriceHolder.getRefMidPricePeriod(),
                isBuySide,
                date,
                orderBookCopy.getOrderBook(isBuySide)))
    }
}