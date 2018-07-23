package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData

interface LimitOrderInputValidator {
    fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun checkVolume(assetPair: AssetPair, order: Order): Boolean
}