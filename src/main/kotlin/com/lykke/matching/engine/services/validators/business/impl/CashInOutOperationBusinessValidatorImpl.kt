package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component("CashInOutOperationBusinessValidator")
class CashInOutOperationBusinessValidatorImpl(private val balancesHolder: BalancesHolder) : CashInOutOperationBusinessValidator {
    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationBusinessValidatorImpl::class.java.name)
    }

    override fun performValidation(cashInOutContext: CashInOutContext) {
        isBalanceValid(cashInOutContext)
    }

    private fun isBalanceValid(cashInOutContext: CashInOutContext) {
        val amount = cashInOutContext.cashInOutOperation.amount
        if (amount < BigDecimal.ZERO) {
            val asset = cashInOutContext.asset
            val balance = balancesHolder.getBalance(cashInOutContext.cashInOutOperation.clientId, asset!!.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(cashInOutContext.cashInOutOperation.clientId, asset.assetId)
            if (NumberUtils.setScaleRoundHalfUp(balance - reservedBalance + amount, asset.accuracy) < BigDecimal.ZERO) {
                LOGGER.info("Cash out operation (${cashInOutContext.cashInOutOperation.externalId}) " +
                        "for client ${cashInOutContext.cashInOutOperation.clientId} asset ${asset.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(amount)}: low balance $balance, " +
                        "reserved balance $reservedBalance")
                throw ValidationException(ValidationException.Validation.LOW_BALANCE)
            }
        }

    }
}