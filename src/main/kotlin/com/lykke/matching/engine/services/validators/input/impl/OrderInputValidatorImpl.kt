package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.OrderInputValidator
import org.springframework.stereotype.Component

@Component
class OrderInputValidatorImpl(val applicationSettingsHolder: ApplicationSettingsHolder): OrderInputValidator {
    override fun validateAsset(assetPair: AssetPair?, assetPairId: String) {
        if (assetPair == null) {
            throw OrderValidationException(OrderStatus.UnknownAsset, "Unable to find asset pair $assetPairId")
        }

        if (applicationSettingsHolder.isAssetDisabled(assetPair.baseAssetId) || applicationSettingsHolder.isAssetDisabled(assetPair.quotingAssetId)) {
            throw OrderValidationException(OrderStatus.DisabledAsset, "disabled asset")
        }
    }

    override fun checkMinVolume(order: Order, assetPair: AssetPair): Boolean {
        val volume = order.getAbsVolume()
        val minVolume = if (order.isStraight()) assetPair.minVolume else assetPair.minInvertedVolume
        return minVolume == null || volume >= minVolume
    }


}