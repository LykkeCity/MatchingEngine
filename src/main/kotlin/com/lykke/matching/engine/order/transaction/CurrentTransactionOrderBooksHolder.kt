package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.matching.UpdatedOrderBookAndOrder
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
    val outgoingOrderBooks = ArrayList<OrderBookData>()

    fun getOrPutOrderCopyWrapper(limitOrder: LimitOrder): CopyWrapper<LimitOrder> {
        return orderCopyWrappersByOriginalOrder.getOrPut(limitOrder) {
            val copyWrapper = (if (ordersService is CurrentTransactionOrderBooksHolder) {
                ordersService.getOrderCopyWrapper(limitOrder)
            } else {
                null
            })?.let {
                CopyWrapper(it.origin, it.copy.copy())
            }

            addChangedSide(limitOrder)
            copyWrapper ?: CopyWrapper(limitOrder)
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
        val ordersToSaveByExternalId = HashMap(newOrdersByExternalId)
        val ordersToRemove = ArrayList(removeOrdersByStatus.values.flatMap { it })

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
        if (ordersService is CurrentTransactionOrderBooksHolder) {
            ordersService.orderCopyWrappersByOriginalOrder.putAll(this.orderCopyWrappersByOriginalOrder)
            this.orderCopyWrappersByOriginalOrder.keys.forEach {
                if (it.isBuySide()) {
                    ordersService.changedBuySides.add(it.assetPairId)
                } else {
                    ordersService.changedSellSides.add(it.assetPairId)
                }
            }
        }

        if (ordersService is GenericLimitOrderService) {
            orderCopyWrappersByOriginalOrder.forEach { it.value.applyToOrigin() }
        }

        assetOrderBookCopiesByAssetPairId.forEach { assetPairId, orderBook ->
            ordersService.setOrderBook(assetPairId, orderBook)
            if (ordersService is GenericLimitOrderService) {
                 if (changedBuySides.contains(assetPairId)) {
                    processChangedOrderBookSide(orderBook, true, date)
                }
                if (changedSellSides.contains(assetPairId)) {
                    processChangedOrderBookSide(orderBook, false, date)
                }
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