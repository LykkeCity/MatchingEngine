package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashInOutOperationInputValidatorImpl constructor(private val balancesHolder: BalancesHolder,
                                                       private val applicationSettingsCache: ApplicationSettingsCache) : CashInOutOperationInputValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationInputValidatorImpl::class.java.name)
    }

    override fun performValidation(cashInOutParsedData: CashInOutParsedData) {
        val cashInOutContext = cashInOutParsedData.messageWrapper.context as CashInOutContext
        isAssetExist(cashInOutContext, cashInOutParsedData.assetId)
        isFeeValid(cashInOutContext.cashInOutOperation.feeInstructions)
        isAssetEnabled(cashInOutContext)
        isBalanceValid(cashInOutContext)
        isVolumeAccuracyValid(cashInOutContext)
    }

    private fun isAssetExist(cashInOutContext: CashInOutContext, inputAssetId: String) {
        if (cashInOutContext.asset == null) {
            LOGGER.info("Asset with id: $inputAssetId does not exist, cash in/out operation; ${cashInOutContext.id}), " +
                    "for client ${cashInOutContext.clientId}")
            throw ValidationException(ValidationException.Validation.UNKNOWN_ASSET)
        }
    }

    private fun isBalanceValid(cashInOutContext: CashInOutContext) {
        val amount = cashInOutContext.cashInOutOperation.amount
        if (amount < BigDecimal.ZERO) {
            val asset = cashInOutContext.asset
            val balance = balancesHolder.getBalance(cashInOutContext.clientId, asset!!.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashInOutContext.clientId, asset.assetId)
            if (NumberUtils.setScaleRoundHalfUp(balance - reservedBalance + amount, asset.accuracy) < BigDecimal.ZERO) {
                LOGGER.info("Cash out operation (${cashInOutContext.id}) " +
                        "for client ${cashInOutContext.clientId} asset ${asset.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(amount)}: low balance $balance, " +
                        "reserved balance $reservedBalance")
                throw ValidationException(ValidationException.Validation.LOW_BALANCE)
            }
        }

    }

    private fun isVolumeAccuracyValid(cashInOutContext: CashInOutContext) {
        val amount = cashInOutContext.cashInOutOperation.amount
        val asset = cashInOutContext.asset
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(amount,
                asset!!.accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy is invalid client: ${cashInOutContext.clientId}, " +
                    "asset: ${asset.assetId}, volume: $amount")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun isAssetEnabled(cashInOutContext: CashInOutContext) {
        val amount = cashInOutContext.cashInOutOperation.amount
        if (amount < BigDecimal.ZERO && applicationSettingsCache.isAssetDisabled(cashInOutContext.asset!!.assetId)) {
            LOGGER.info("Cash out operation (${cashInOutContext.id}) for client ${cashInOutContext.clientId} " +
                    "asset ${cashInOutContext.asset.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(amount)}: disabled asset")
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(feeInstructions: List<NewFeeInstruction>?) {
        if (!checkFee(null, feeInstructions)) {
            throw ValidationException(ValidationException.Validation.INVALID_FEE, "invalid fee for client")
        }
    }
}