package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashOperationValidatorImpl @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                         private val assetsHolder: AssetsHolder,
                                                         private val applicationSettingsCache: ApplicationSettingsCache) : CashOperationValidator {

    override fun performValidation(cashOperation: ProtocolMessages.CashOperation){
        isBalanceValid(cashOperation)
    }

    private fun isBalanceValid(cashOperation: ProtocolMessages.CashOperation) {
        if (cashOperation.amount < 0) {
            val balance = balancesHolder.getBalance(cashOperation.clientId, cashOperation.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashOperation.clientId, cashOperation.assetId)
            if (balance - reservedBalance < BigDecimal.valueOf(cashOperation.amount).abs()) {
                throw ValidationException(ValidationException.Validation.LOW_BALANCE, "Cash out operation (${cashOperation.uid})" +
                        "for client ${cashOperation.clientId} asset ${cashOperation.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(cashOperation.amount)}: " +
                        "low balance $balance, reserved balance $reservedBalance")
            }
        }
    }


}

