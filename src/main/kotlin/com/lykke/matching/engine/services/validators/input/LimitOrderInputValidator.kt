package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData

interface LimitOrderInputValidator {
    fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData)
    fun validateLimitOrder(isTrustedClient: Boolean,
                           order: LimitOrder,
                           assetPair: AssetPair,
                           baseAssetDisabled: Boolean,
                           quotingAssetDisabled: Boolean,
                           baseAsset: Asset)
    fun checkVolume(order: Order, assetPair: AssetPair): Boolean
}