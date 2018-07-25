package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashTransferOperationBusinessValidatorImpl (private val balancesHolder: BalancesHolder): CashTransferOperationBusinessValidator {
    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationBusinessValidatorImpl::class.java.name)

    }

    override fun performValidation(cashTransferContext: CashTransferContext) {
        validateBalanceValid(cashTransferContext)
        validateOverdraftLimitPositive(cashTransferContext)
    }

    private fun validateOverdraftLimitPositive(cashTransferContext: CashTransferContext) {
        val transferOperation = cashTransferContext.transferOperation
        val overdraftLimit = cashTransferContext.transferOperation.overdraftLimit

        if (overdraftLimit != null && overdraftLimit.signum() == -1) {
            throw ValidationException(ValidationException.Validation.OVERDRAFT_LIMIT_IS_NEGATIVE, "ClientId:${transferOperation.fromClientId}, " +
                    "asset:${transferOperation.asset}, volume:${transferOperation.volume}")
        }
    }

    private fun validateBalanceValid(cashTransferContext: CashTransferContext) {
        val transferOperation = cashTransferContext.transferOperation
        val balanceOfFromClient = balancesHolder.getBalance(transferOperation.fromClientId, transferOperation.asset)
        val reservedBalanceOfFromClient = balancesHolder.getReservedBalance(transferOperation.fromClientId, transferOperation.asset)
        val overdraftLimit = if (transferOperation.overdraftLimit != null) -transferOperation.overdraftLimit else BigDecimal.ZERO
        if (balanceOfFromClient - reservedBalanceOfFromClient - transferOperation.volume < overdraftLimit) {
            LOGGER.info("Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} " +
                    "to client ${transferOperation.toClientId}, asset ${transferOperation.asset}, " +
                    "volume: ${NumberUtils.roundForPrint(transferOperation.volume)}: " +
                    "low balance for client ${transferOperation.fromClientId}")

            throw ValidationException(ValidationException.Validation.LOW_BALANCE, "ClientId:${transferOperation.fromClientId}, " +
                    "asset:${transferOperation.asset}, volume:${transferOperation.volume}")
        }
    }
}