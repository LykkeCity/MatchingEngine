package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashInOutOperationValidatorImpl constructor(private val balancesHolder: BalancesHolder,
                                                  private val assetsHolder: AssetsHolder,
                                                  private val applicationSettingsCache: ApplicationSettingsCache) : CashInOutOperationValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(cashInOutOperation: ProtocolMessages.CashInOutOperation, feeInstructions: List<NewFeeInstruction>) {
        isFeeValid(feeInstructions)
        isAssetEnabled(cashInOutOperation)
        isBalanceValid(cashInOutOperation)
        isVolumeAccuracyValid(cashInOutOperation)
    }

    private fun isBalanceValid(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
        if (cashInOutOperation.volume < 0) {
            val balance = balancesHolder.getBalance(cashInOutOperation.clientId, cashInOutOperation.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashInOutOperation.clientId, cashInOutOperation.assetId)
            if (NumberUtils.setScaleRoundHalfUp(balance - reservedBalance + BigDecimal.valueOf(cashInOutOperation.volume), assetsHolder.getAsset(cashInOutOperation.assetId).accuracy) < BigDecimal.ZERO) {
                LOGGER.info("Cash out operation (${cashInOutOperation.id}) " +
                        "for client ${cashInOutOperation.clientId} asset ${cashInOutOperation.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(cashInOutOperation.volume)}: low balance $balance, " +
                        "reserved balance $reservedBalance")
                throw ValidationException(ValidationException.Validation.LOW_BALANCE)
            }
        }

    }

    private fun isVolumeAccuracyValid(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(cashInOutOperation.volume),
                assetsHolder.getAsset(cashInOutOperation.assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy is invalid client: ${cashInOutOperation.clientId}, " +
                    "asset: ${cashInOutOperation.assetId}, volume: ${cashInOutOperation.volume}")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun isAssetEnabled(cashInOutOperation: ProtocolMessages.CashInOutOperation) {
         if (cashInOutOperation.volume < 0 && applicationSettingsCache.isAssetDisabled(cashInOutOperation.assetId)) {
             LOGGER.info("Cash out operation (${cashInOutOperation.id}) for client ${cashInOutOperation.clientId} " +
                     "asset ${cashInOutOperation.assetId}, " +
                     "volume: ${NumberUtils.roundForPrint(cashInOutOperation.volume)}: disabled asset")
             throw ValidationException (ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(feeInstructions: List<NewFeeInstruction>) {
        if (!checkFee(null, feeInstructions)) {
            throw ValidationException(ValidationException.Validation.INVALID_FEE, "invalid fee for client")
        }
    }
}