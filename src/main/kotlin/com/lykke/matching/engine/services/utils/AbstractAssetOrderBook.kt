package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.NewLimitOrder

interface AbstractAssetOrderBook {
    fun copy(): AbstractAssetOrderBook
    fun removeOrder(order: NewLimitOrder): Boolean
}