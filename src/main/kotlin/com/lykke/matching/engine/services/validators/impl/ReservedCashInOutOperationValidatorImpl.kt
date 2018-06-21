package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class ReservedCashInOutOperationValidatorImpl @Autowired constructor(private val assetsHolder: AssetsHolder,
                                                                     private val balancesHolder: BalancesHolder) : ReservedCashInOutOperationValidator {
    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(message: ProtocolMessages.ReservedCashInOutOperation) {
        isVolumeAccuracyValid(message)

        if (message.reservedVolume < 0) {
            isBalanceValid(message)
        } else {
            isReservedVolumeValid(message)
        }
    }

    private fun isReservedVolumeValid(message: ProtocolMessages.ReservedCashInOutOperation) {
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy
        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

        val balance = balancesHolder.getBalance(message.clientId, message.assetId)
        if (NumberUtils.setScaleRoundHalfUp(balance - reservedBalance - BigDecimal.valueOf(message.reservedVolume), accuracy) < BigDecimal.ZERO) {
            LOGGER.info("Reserved cash in operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: low balance $balance, " +
                    "current reserved balance $reservedBalance")
            throw ValidationException(ValidationException.Validation.RESERVED_VOLUME_HIGHER_THAN_BALANCE)
        }
    }

    private fun isBalanceValid(message: ProtocolMessages.ReservedCashInOutOperation) {
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy
        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

        if (NumberUtils.setScaleRoundHalfUp(reservedBalance + BigDecimal.valueOf(message.reservedVolume), accuracy) < BigDecimal.ZERO) {
            LOGGER.info("Reserved cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: low reserved balance $reservedBalance")
            throw ValidationException(ValidationException.Validation.LOW_BALANCE)
        }
    }

    private fun isVolumeAccuracyValid(message: ProtocolMessages.ReservedCashInOutOperation) {
        val assetId = message.assetId
        val volume = message.reservedVolume
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(volume), assetsHolder.getAsset(assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid, assetId $assetId, volume $volume")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }
}