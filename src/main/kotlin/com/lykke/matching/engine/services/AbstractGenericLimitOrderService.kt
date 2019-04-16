package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Date

interface AbstractGenericLimitOrderService<T : AbstractAssetOrderBook> {
    fun getOrderBook(assetPairId: String): T
    fun setOrderBook(assetPairId: String, assetOrderBook: T)
    fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus? = null, date: Date? = null)
    fun addOrders(orders: Collection<LimitOrder>)
    fun getTotalSize(): Int
}