package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CashOperationValidatorImpl @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                         private val assetsHolder: AssetsHolder,
                                                         private val applicationSettingsCache: ApplicationSettingsCache) : CashOperationValidator {

    override fun performValidation(cashOperation: ProtocolMessages.CashOperation){
        isAssetEnabled(cashOperation)
        isBalanceValid(cashOperation)
        isAccuracyValid(cashOperation)
    }

    private fun isBalanceValid(cashOperation: ProtocolMessages.CashOperation) {
        if (cashOperation.amount < 0) {
            val balance = balancesHolder.getBalance(cashOperation.clientId, cashOperation.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashOperation.clientId, cashOperation.assetId)
            if (balance - reservedBalance < Math.abs(cashOperation.amount)) {
                throw ValidationException(ValidationException.Validation.LOW_BALANCE, "Cash out operation (${cashOperation.uid})" +
                        "for client ${cashOperation.clientId} asset ${cashOperation.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(cashOperation.amount)}: " +
                        "low balance $balance, reserved balance $reservedBalance")
            }
        }
    }

    private fun isAccuracyValid(cashOperation: ProtocolMessages.CashOperation){

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(cashOperation.amount, assetsHolder.getAsset(cashOperation.assetId).accuracy)

        if (!volumeValid) {
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY,
                    "Amount accuracy is invalid clientId: ${cashOperation.clientId}, amount  $cashOperation.amount")
        }
    }

    private fun isAssetEnabled(cashOperation: ProtocolMessages.CashOperation){
        if (cashOperation.amount < 0 && applicationSettingsCache.isAssetDisabled(cashOperation.assetId)) {
            throw ValidationException (ValidationException.Validation.DISABLED_ASSET,
                    "Cash out operation (${cashOperation.uid}) for client ${cashOperation.clientId} asset ${cashOperation.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(cashOperation.amount)}: disabled asset")
        }
    }
}

