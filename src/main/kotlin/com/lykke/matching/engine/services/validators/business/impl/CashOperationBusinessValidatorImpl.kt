package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.business.CashOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashOperationBusinessValidatorImpl(private val balancesHolder: BalancesHolder): CashOperationBusinessValidator {
    override fun performValidation(cashOperationContext: CashOperationContext){
        isBalanceValid(cashOperationContext)
    }

    private fun isBalanceValid(cashOperationContext: CashOperationContext) {
        val walletOperation = cashOperationContext.walletOperation
        if (walletOperation.amount < BigDecimal.ZERO) {
            val balance = balancesHolder.getBalance(cashOperationContext.clientId, walletOperation.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashOperationContext.clientId, walletOperation.assetId)
            if (balance - reservedBalance < walletOperation.amount.abs()) {
                throw ValidationException(ValidationException.Validation.LOW_BALANCE, "Cash out operation (${cashOperationContext.uid})" +
                        "for client ${cashOperationContext.clientId} asset ${walletOperation.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(walletOperation.amount)}: " +
                        "low balance $balance, reserved balance $reservedBalance")
            }
        }
    }
}