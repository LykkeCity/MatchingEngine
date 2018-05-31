package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.services.AssetOrderBook

interface MultiLimitOrderValidator {
    fun performValidation(order: NewLimitOrder, assetPair: AssetPair, orderBook: AssetOrderBook)
}