package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.Date

@Component
class CashTransferOperationValidatorImpl @Autowired constructor(private val balancesHolder: BalancesHolder,
                                                                private val assetsHolder: AssetsHolder,
                                                                private val applicationSettingsCache: ApplicationSettingsCache)
    : CashTransferOperationValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(cashTransferOperation: ProtocolMessages.CashTransferOperation,
                                   operationId: String, feeInstructions: List<NewFeeInstruction>,
                                   feeInstruction: FeeInstruction?) {
        val operation = TransferOperation(operationId, cashTransferOperation.id,
                cashTransferOperation.fromClientId, cashTransferOperation.toClientId,
                cashTransferOperation.assetId, Date(cashTransferOperation.timestamp),
                BigDecimal.valueOf(cashTransferOperation.volume), BigDecimal.valueOf(cashTransferOperation.overdraftLimit),
                listOfFee(feeInstruction, feeInstructions))

        isAssetEnabled(cashTransferOperation)
        isFeeValid(cashTransferOperation, feeInstruction, feeInstructions)
        isBalanceValid(cashTransferOperation, operation)
        isVolumeAccuracyValid(cashTransferOperation)
    }

    private fun isBalanceValid(cashTransferOperation: ProtocolMessages.CashTransferOperation, operation: TransferOperation) {
        val balanceOfFromClient = balancesHolder.getBalance(cashTransferOperation.fromClientId, cashTransferOperation.assetId)
        val reservedBalanceOfFromClient = balancesHolder.getReservedBalance(cashTransferOperation.fromClientId, cashTransferOperation.assetId)
        val overdraftLimit = if (operation.overdraftLimit != null) - operation.overdraftLimit else BigDecimal.ZERO
        if (balanceOfFromClient - reservedBalanceOfFromClient - operation.volume < overdraftLimit) {
            LOGGER.info("Cash transfer operation (${cashTransferOperation.id}) from client ${cashTransferOperation.fromClientId} " +
                    "to client ${cashTransferOperation.toClientId}, asset ${cashTransferOperation.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(cashTransferOperation.volume)}: " +
                    "low balance for client ${cashTransferOperation.fromClientId}")

            throw ValidationException(ValidationException.Validation.LOW_BALANCE, "ClientId:${cashTransferOperation.fromClientId}, " +
                    "asset:${cashTransferOperation.assetId}, volume:${cashTransferOperation.volume}")
        }
    }

    private fun isAssetEnabled(cashTransferOperation: ProtocolMessages.CashTransferOperation) {
        if (applicationSettingsCache.isAssetDisabled(cashTransferOperation.assetId)) {
            LOGGER.info("Cash transfer operation (${cashTransferOperation.id}) from client ${cashTransferOperation.fromClientId} " +
                    "to client ${cashTransferOperation.toClientId}, asset ${cashTransferOperation.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(cashTransferOperation.volume)}: disabled asset")
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isFeeValid(cashTransferOperation: ProtocolMessages.CashTransferOperation, feeInstruction: FeeInstruction?,
                           feeInstructions: List<NewFeeInstruction>){
        if (!checkFee(feeInstruction, feeInstructions)) {
            LOGGER.info("Fee is invalid  from client: ${cashTransferOperation.fromClientId}, to cloent: ${cashTransferOperation.toClientId}")
            throw ValidationException(ValidationException.Validation.INVALID_FEE, "invalid fee for client")
        }
    }

    private fun isVolumeAccuracyValid(cashTransferOperation: ProtocolMessages.CashTransferOperation) {

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(cashTransferOperation.volume),
                assetsHolder.getAsset(cashTransferOperation.assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid fromClient  ${cashTransferOperation.fromClientId}, " +
                    "to client ${cashTransferOperation.toClientId} assetId: ${cashTransferOperation.assetId}, volume: ${cashTransferOperation.volume}")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }
}