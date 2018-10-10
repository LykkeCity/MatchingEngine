package com.lykke.matching.engine.database.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.common.OrderBookSide
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import org.apache.log4j.Logger

fun mapOrdersToOrderBookPersistenceDataList(orders: Collection<LimitOrder>, orderBooksSides: Collection<OrderBookSide>, log: Logger): List<OrderBookPersistenceData> {
    val orderBooks = mutableMapOf<String, MutableMap<Boolean, MutableCollection<LimitOrder>>>()
    orders.forEach { order ->
        orderBooks.getOrPut(order.assetPairId) { mutableMapOf() }
                .getOrPut(order.isBuySide()) { mutableListOf() }
                .add(order)
    }

    val mutableOrderBooksSides = orderBooksSides.toMutableList()
    val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
    orderBooks.forEach {assetPairId, sideOrders ->
        sideOrders.forEach { isBuy, orders ->
            mutableOrderBooksSides.remove(OrderBookSide(assetPairId, isBuy))
            orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, isBuy, orders))
        }
    }
    mutableOrderBooksSides.forEach { orderBooksSide ->
        log.info("Orders $orderBooksSide are absent in primary db and will be removed from secondary db")
        orderBookPersistenceDataList.add(OrderBookPersistenceData(orderBooksSide.assetPairId, orderBooksSide.isBuySide, emptyList()))
    }

    return orderBookPersistenceDataList
}

fun mapOrdersToOrderBookPersistenceDataList(orders: Collection<LimitOrder>, log: Logger) = mapOrdersToOrderBookPersistenceDataList(orders, emptyList(), log)


