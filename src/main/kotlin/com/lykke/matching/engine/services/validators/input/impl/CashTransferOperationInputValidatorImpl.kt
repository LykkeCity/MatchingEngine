package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CashTransferOperationInputValidatorImpl @Autowired constructor(private val applicationSettingsHolder: ApplicationSettingsHolder)
    : CashTransferOperationInputValidator {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(CashTransferOperationInputValidatorImpl::class.java.name)
    }

    override fun performValidation(cashTransferParsedData: CashTransferParsedData) {
        val cashTransferContext = getCashTransferContext(cashTransferParsedData)
        isAssetExist(cashTransferParsedData)
        isAssetEnabled(cashTransferParsedData)
        isFeeValid(cashTransferContext, cashTransferParsedData.feeInstruction, cashTransferParsedData.feeInstructions)
        isVolumeAccuracyValid(cashTransferParsedData)
    }

    private fun isAssetExist(cashTransferParsedData: CashTransferParsedData) {
        val cashTransferContext = getCashTransferContext(cashTransferParsedData)

        if (cashTransferContext.transferOperation.asset == null) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} " +
                    "to client ${transferOperation.toClientId}, asset ${cashTransferParsedData.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(transferOperation.volume)}: asset with id: ${cashTransferParsedData.assetId}")
            throw ValidationException(ValidationException.Validation.UNKNOWN_ASSET)
        }
    }

    private fun isAssetEnabled(cashTransferParsedData: CashTransferParsedData) {
        val cashTransferContext = getCashTransferContext(cashTransferParsedData)

        if (applicationSettingsHolder.isAssetDisabled(cashTransferContext.transferOperation.asset!!.assetId)) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} " +
                    "to client ${transferOperation.toClientId}, asset ${cashTransferParsedData.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(transferOperation.volume)}: disabled asset")
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(cashTransferContext: CashTransferContext, feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>) {
        if (!checkFee(feeInstruction, feeInstructions)) {
            val transferOperation = cashTransferContext.transferOperation
            LOGGER.info("Fee is invalid  from client: ${transferOperation.fromClientId}, to client: ${transferOperation.toClientId}")
            throw ValidationException(ValidationException.Validation.INVALID_FEE)
        }
    }

    private fun isVolumeAccuracyValid(cashTransferParsedData: CashTransferParsedData) {
        val cashTransferContext = getCashTransferContext(cashTransferParsedData)

        val transferOperation = cashTransferContext.transferOperation
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(transferOperation.volume,
                transferOperation.asset!!.accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid fromClient  ${transferOperation.fromClientId}, " +
                    "to client ${transferOperation.toClientId} assetId: ${cashTransferParsedData.assetId}, volume: ${transferOperation.volume}")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun getCashTransferContext(cashTransferParsedData: CashTransferParsedData) =
            cashTransferParsedData.messageWrapper.context as CashTransferContext
}