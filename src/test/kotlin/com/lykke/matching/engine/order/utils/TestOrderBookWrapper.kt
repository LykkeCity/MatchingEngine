package com.lykke.matching.engine.order.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService

class TestOrderBookWrapper(private val genericLimitOrderService:  GenericLimitOrderService,
                           private val testFileOrderDatabaseAccessor: TestFileOrderDatabaseAccessor,
                           private val genericStopLimitOrderService: GenericStopLimitOrderService,
                           private val stopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor) {


    fun addLimitOrder(limitOrder: LimitOrder) {
        testFileOrderDatabaseAccessor.addLimitOrder(limitOrder)

        genericLimitOrderService.addOrder(limitOrder)
        val orderBook = genericLimitOrderService.getOrderBook(limitOrder.assetPairId)
        orderBook.addOrder(limitOrder)
        genericLimitOrderService.setOrderBook(limitOrder.assetPairId, orderBook)
    }

    fun addStopLimitOrder(limitOrder: LimitOrder) {
        stopOrderBookDatabaseAccessor.addStopLimitOrder(limitOrder)

        genericStopLimitOrderService.addOrder(limitOrder)
        val orderBook = genericStopLimitOrderService.getOrderBook(limitOrder.assetPairId)
        orderBook.addOrder(limitOrder)
        genericStopLimitOrderService.setOrderBook(limitOrder.assetPairId, orderBook)
    }
}