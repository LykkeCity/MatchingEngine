package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date

interface AbstractGenericLimitOrderService<T: AbstractAssetOrderBook> {
    fun getOrderBook(assetPairId: String): T
    fun cancelLimitOrders(orders: Collection<LimitOrder>, date: Date)
    fun setOrderBook(assetPairId: String, assetOrderBook: T)
    fun updateOrderBook(assetPairId: String, isBuy: Boolean)
}