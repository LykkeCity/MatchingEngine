package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder

abstract class AbstractAssetOrderBook(val assetPairId: String) {
    abstract fun copy(): AbstractAssetOrderBook
    abstract fun removeOrder(order: LimitOrder): Boolean
    abstract fun getOrderBook(isBuySide: Boolean): Collection<LimitOrder>
    abstract fun addOrder(order: LimitOrder): Boolean
    fun getBuyOrderBook() = getOrderBook(true)
    fun getSellOrderBook() = getOrderBook(false)
}