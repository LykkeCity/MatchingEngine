package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashInOutOperationValidatorImpl constructor(private val balancesHolder: BalancesHolder,
                                                  private val applicationSettingsCache: ApplicationSettingsCache) : CashInOutOperationValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(cashInOutContext: CashInOutContext) {
        isFeeValid(cashInOutContext.feeInstructions)
        isAssetEnabled(cashInOutContext)
        isBalanceValid(cashInOutContext)
        isVolumeAccuracyValid(cashInOutContext)
    }

    private fun isBalanceValid(cashInOutContext: CashInOutContext) {
        if (cashInOutContext.volume < BigDecimal.ZERO) {
            val asset = cashInOutContext.asset
            val balance = balancesHolder.getBalance(cashInOutContext.clientId, asset.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashInOutContext.clientId, asset.assetId)
            if (NumberUtils.setScaleRoundHalfUp(balance - reservedBalance + cashInOutContext.volume, asset.accuracy) < BigDecimal.ZERO) {
                LOGGER.info("Cash out operation (${cashInOutContext.id}) " +
                        "for client ${cashInOutContext.clientId} asset ${asset.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(cashInOutContext.volume)}: low balance $balance, " +
                        "reserved balance $reservedBalance")
                throw ValidationException(ValidationException.Validation.LOW_BALANCE)
            }
        }

    }

    private fun isVolumeAccuracyValid(cashInOutContext: CashInOutContext) {
        val asset = cashInOutContext.asset
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(cashInOutContext.volume,
                asset.accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy is invalid client: ${cashInOutContext.clientId}, " +
                    "asset: ${asset.assetId}, volume: ${cashInOutContext.volume}")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun isAssetEnabled(cashInOutContext: CashInOutContext) {
        if (cashInOutContext.volume < BigDecimal.ZERO && applicationSettingsCache.isAssetDisabled(cashInOutContext.asset.assetId)) {
            LOGGER.info("Cash out operation (${cashInOutContext.id}) for client ${cashInOutContext.clientId} " +
                    "asset ${cashInOutContext.asset.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(cashInOutContext.volume)}: disabled asset")
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(feeInstructions: List<NewFeeInstruction>) {
        if (!checkFee(null, feeInstructions)) {
            throw ValidationException(ValidationException.Validation.INVALID_FEE, "invalid fee for client")
        }
    }
}