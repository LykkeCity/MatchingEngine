package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.AssetOrderBook

interface MultiLimitOrderValidator {
    fun performValidation(order: LimitOrder, assetPair: AssetPair, orderBook: AssetOrderBook)
}