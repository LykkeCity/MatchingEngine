package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook

interface AbstractGenericLimitOrderService<T: AbstractAssetOrderBook> {
    fun getOrderBook(assetPairId: String): T
    fun cancelLimitOrders(orders: Collection<NewLimitOrder>)
    fun setOrderBook(assetPairId: String, assetOrderBook: T)
}