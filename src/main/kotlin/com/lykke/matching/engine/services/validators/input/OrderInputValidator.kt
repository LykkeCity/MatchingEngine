package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Order

interface OrderInputValidator {
    fun checkMinVolume(order: Order, assetPair: AssetPair): Boolean
    fun validateAsset(assetPair: AssetPair?, assetPairId: String)
}