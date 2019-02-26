package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.matching.UpdatedOrderBookAndOrder
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import java.util.Date
import java.util.HashMap
import java.util.concurrent.PriorityBlockingQueue

class CurrentTransactionOrderBooksHolder(private val genericLimitOrderService: GenericLimitOrderService)
    : AbstractTransactionOrderBooksHolder<AssetOrderBook, GenericLimitOrderService>(genericLimitOrderService) {

    private val orderCopyWrappersByOriginalOrder = HashMap<LimitOrder, CopyWrapper<LimitOrder>>()

    val tradeInfoList = mutableListOf<TradeInfo>()
    val outgoingOrderBooks = ArrayList<OrderBookData>()

    fun getOrPutOrderCopyWrapper(limitOrder: LimitOrder): CopyWrapper<LimitOrder> {
        return orderCopyWrappersByOriginalOrder.getOrPut(limitOrder) {
            addChangedSide(limitOrder)
            CopyWrapper(limitOrder)
        }
    }

    override fun getPersistenceData(): OrderBooksPersistenceData {
        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSaveByExternalId = HashMap(newOrdersByExternalId)
        val ordersToRemove = ArrayList<LimitOrder>(completedOrders.size + cancelledOrders.size + replacedOrders.size)

        ordersToRemove.addAll(completedOrders)
        ordersToRemove.addAll(cancelledOrders)
        ordersToRemove.addAll(replacedOrders)

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            if (changedBuySides.contains(assetPairId)) {
                readOrderBookSidePersistenceData(orderBook,
                        true,
                        orderBookPersistenceDataList,
                        ordersToSaveByExternalId)
            }
            if (changedSellSides.contains(assetPairId)) {
                readOrderBookSidePersistenceData(orderBook,
                        false,
                        orderBookPersistenceDataList,
                        ordersToSaveByExternalId)
            }
        }

        return OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSaveByExternalId.values, ordersToRemove)
    }

    private fun readOrderBookSidePersistenceData(orderBook: AssetOrderBook,
                                                 isBuySide: Boolean,
                                                 orderBookPersistenceDataList: MutableList<OrderBookPersistenceData>,
                                                 ordersToSaveByExternalId: MutableMap<String, LimitOrder>) {
        val updatedOrders = createUpdatedOrderBookAndOrder(orderBook.getOrderBook(isBuySide))
        orderBookPersistenceDataList.add(OrderBookPersistenceData(orderBook.assetPairId, isBuySide, updatedOrders.updatedOrderBook))
        updatedOrders.updatedOrder?.let { updatedOrder ->
            if (newOrdersByExternalId.containsKey(updatedOrder.externalId)) {
                ordersToSaveByExternalId.remove(updatedOrder.externalId)
            }
            ordersToSaveByExternalId[updatedOrder.externalId] = updatedOrder
        }
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
        orderCopyWrappersByOriginalOrder.forEach { it.value.applyToOrigin() }
        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            genericLimitOrderService.setOrderBook(assetPairId, orderBook)
            if (changedBuySides.contains(assetPairId)) {
                processChangedOrderBookSide(orderBook, true, date)
            }
            if (changedSellSides.contains(assetPairId)) {
                processChangedOrderBookSide(orderBook, false, date)
            }
        }
    }

    private fun processChangedOrderBookSide(orderBook: AssetOrderBook,
                                            isBuySide: Boolean,
                                            date: Date) {
        val assetPairId = orderBook.assetPairId
        val price = if (isBuySide) orderBook.getBidPrice() else orderBook.getAskPrice()
        tradeInfoList.add(TradeInfo(assetPairId, isBuySide, price, date))
        outgoingOrderBooks.add(OrderBookData(orderBook.getOrderBook(isBuySide).toArray(emptyArray<LimitOrder>()),
                assetPairId,
                date,
                isBuySide))
    }

    class OrderBookData(val orders: Array<LimitOrder>,
                        val assetPair: String,
                        val date: Date,
                        val isBuySide: Boolean)
}