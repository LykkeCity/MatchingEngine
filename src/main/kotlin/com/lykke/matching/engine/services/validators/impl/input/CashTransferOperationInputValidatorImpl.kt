package com.lykke.matching.engine.services.validators.impl.input

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CashTransferOperationInputValidatorImpl @Autowired constructor(private val assetsHolder: AssetsHolder,
                                                                     private val applicationSettingsCache: ApplicationSettingsCache)
    : CashTransferOperationValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationInputValidatorImpl::class.java.name)
    }

    override fun performValidation(cashTransferContext: CashTransferContext) {
        isAssetExist(cashTransferContext)
        isAssetEnabled(cashTransferContext)
        isFeeValid(cashTransferContext)
        isVolumeAccuracyValid(cashTransferContext)
    }

    private fun isAssetExist(cashTransferContext: CashTransferContext) {
        if (cashTransferContext.asset == null) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} " +
                    "to client ${transferOperation.toClientId}, asset ${transferOperation.asset}, " +
                    "volume: ${NumberUtils.roundForPrint(transferOperation.volume)}: asset with id: ${cashTransferContext.inputAssetId}")
            throw ValidationException(ValidationException.Validation.UNKNOWN_ASSET)
        }
    }

    private fun isAssetEnabled(cashTransferContext: CashTransferContext) {
        if (applicationSettingsCache.isAssetDisabled(cashTransferContext.asset!!.assetId)) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} " +
                    "to client ${transferOperation.toClientId}, asset ${transferOperation.asset}, " +
                    "volume: ${NumberUtils.roundForPrint(transferOperation.volume)}: disabled asset")
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(cashTransferContext: CashTransferContext) {
        if (!checkFee(cashTransferContext.feeInstruction, cashTransferContext.feeInstructions)) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Fee is invalid  from client: ${transferOperation.fromClientId}, to client: ${transferOperation.toClientId}")
            throw ValidationException(ValidationException.Validation.INVALID_FEE)
        }
    }

    private fun isVolumeAccuracyValid(cashTransferContext: CashTransferContext) {
        val transferOperation = cashTransferContext.transferOperation
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(transferOperation.volume,
                assetsHolder.getAsset(transferOperation.asset).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid fromClient  ${transferOperation.fromClientId}, " +
                    "to client ${transferOperation.toClientId} assetId: ${transferOperation.asset}, volume: ${transferOperation.volume}")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }
}