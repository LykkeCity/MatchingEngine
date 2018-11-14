package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.order.process.context.StopLimitOrderContext

interface LimitOrderInputValidator {
    fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun validateLimitOrder(isTrustedClient: Boolean,
                           order: LimitOrder,
                           assetPair: AssetPair?,
                           assetPairId: String,
                           baseAsset: Asset?)
    fun validateStopOrder(stopLimitOrderContext: StopLimitOrderContext)
}