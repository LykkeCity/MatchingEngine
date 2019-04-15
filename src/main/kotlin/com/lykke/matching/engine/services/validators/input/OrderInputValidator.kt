package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder

interface OrderInputValidator {
    fun validateAsset(assetPair: AssetPair?, assetPairId: String)

    fun validateFee(order: LimitOrder)

    fun validatePrice(limitOrder: LimitOrder)

    fun validateLimitPrices(order: LimitOrder)

    fun validateValue(order: LimitOrder, assetPair: AssetPair)

    fun validateMaxValue(limitOrder: LimitOrder, assetPair: AssetPair)

    fun validateStopOrderMaxValue(limitOrder: LimitOrder, assetPair: AssetPair)

    fun validateVolume(limitOrder: LimitOrder, assetPair: AssetPair)

    fun validateVolumeAccuracy(limitOrder: LimitOrder, baseAsset: Asset)

    fun validatePriceAccuracy(limitOrder: LimitOrder, assetPair: AssetPair)
}