package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.NewLimitOrder

abstract class AbstractAssetOrderBook(val assetPairId: String) {
    abstract fun copy(): AbstractAssetOrderBook
    abstract fun removeOrder(order: NewLimitOrder): Boolean
    abstract fun getOrderBook(isBuySide: Boolean): Collection<NewLimitOrder>
}