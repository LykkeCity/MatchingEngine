package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.BalanceUpdateValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class BalanceUpdateValidatorImpl @Autowired constructor(private val balancesHolder: BalancesHolder,
                                                        private val assetsHolder: AssetsHolder): BalanceUpdateValidator {

    companion object {
        private val LOGGER = Logger.getLogger(BalanceUpdateValidatorImpl::class.java.name)
    }

    override fun performValidation(message: ProtocolMessages.BalanceUpdate) {
        isBalanceValid(message)
        isAmountAccuracyValid(message)
    }

    private fun isBalanceValid(message: ProtocolMessages.BalanceUpdate) {
        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

        if (reservedBalance > BigDecimal.valueOf(message.amount)) {
            LOGGER.info("Balance (client ${message.clientId}, " +
                    "asset ${message.assetId}, ${NumberUtils.roundForPrint(message.amount)}) " +
                    "is lower that reserved balance ${NumberUtils.roundForPrint(reservedBalance)}")
            throw ValidationException(ValidationException.Validation.BALANCE_LOWER_THAN_RESERVED)
        }
    }

    private fun isAmountAccuracyValid(message: ProtocolMessages.BalanceUpdate) {
        val amount = message.amount
        val assetId = message.assetId
        val amountAccuracyValid = NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(amount), assetsHolder.getAsset(assetId).accuracy)

        if (!amountAccuracyValid) {
            LOGGER.info("Amount accuracy invalid client: ${message.clientId}, asset: $assetId, amount $amount")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }
}