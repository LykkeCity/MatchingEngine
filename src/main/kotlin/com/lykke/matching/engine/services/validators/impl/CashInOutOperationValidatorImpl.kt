package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component

@Component
class CashInOutOperationValidatorImpl constructor(private val balancesHolder: BalancesHolder,
                                                  private val assetsHolder: AssetsHolder,
                                                  private val applicationSettingsCache: ApplicationSettingsCache) : CashInOutOperationValidator {

    override fun performValidation(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
        val feeInstructions = NewFeeInstruction.create(cashInOutOperation.feesList)
        isFeeValid(feeInstructions)
        isAssetEnabled(cashInOutOperation)
        isBalanceValid(cashInOutOperation)
        isVolumeAccuracyValid(cashInOutOperation)
    }

    private fun isBalanceValid(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
        if (cashInOutOperation.volume < 0) {
            val balance = balancesHolder.getBalance(cashInOutOperation.clientId, cashInOutOperation.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashInOutOperation.clientId, cashInOutOperation.assetId)
            if (NumberUtils.parseDouble(balance - reservedBalance + cashInOutOperation.volume, assetsHolder.getAsset(cashInOutOperation.assetId).accuracy).toDouble() < 0.0) {
                throw ValidationException("Cash out operation (${cashInOutOperation.id}) " +
                        "for client ${cashInOutOperation.clientId} asset ${cashInOutOperation.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(cashInOutOperation.volume)}: low balance $balance, " +
                        "reserved balance $reservedBalance", ValidationException.Validation.LOW_BALANCE)
            }
        }

    }

    private fun isVolumeAccuracyValid(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(cashInOutOperation.volume, assetsHolder.getAsset(cashInOutOperation.assetId).accuracy)

        if (!volumeValid) {
            throw ValidationException("Volume accuracy is invalid  client: ${cashInOutOperation.clientId}, " +
                    "asset: ${cashInOutOperation.assetId}, volume: $cashInOutOperation.volume", ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun isAssetEnabled(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
         if (cashInOutOperation.volume < 0 && applicationSettingsCache.isAssetDisabled(cashInOutOperation.assetId)) {
             throw ValidationException ("Cash out operation (${cashInOutOperation.id}) for client ${cashInOutOperation.clientId} asset ${cashInOutOperation.assetId}, " +
                     "volume: ${NumberUtils.roundForPrint(cashInOutOperation.volume)}: disabled asset", ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(feeInstructions: List<NewFeeInstruction>) {
        if (!checkFee(null, feeInstructions)) {
            throw ValidationException("invalid fee for client", ValidationException.Validation.INVALID_FEE)
        }
    }
}