package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashOperationInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashOperationInputValidatorImpl: CashOperationInputValidator {
    override fun performValidation(cashOperationParsedData: CashOperationParsedData) {
        val cashOperationContext = cashOperationParsedData.messageWrapper.context as CashOperationContext

        isAssetEnabled(cashOperationContext)
        isAccuracyValid(cashOperationContext)
    }

    private fun isAccuracyValid(cashOperationContext: CashOperationContext){
        val walletOperation = cashOperationContext.walletOperation

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(walletOperation.amount,
                cashOperationContext.asset.accuracy)

        if (!volumeValid) {
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY,
                    "Amount accuracy is invalid clientId: ${cashOperationContext.clientId}, amount  ${walletOperation.amount}")
        }
    }

    private fun isAssetEnabled(cashOperationContext: CashOperationContext){
        val walletOperation = cashOperationContext.walletOperation

        if (walletOperation.amount < BigDecimal.ZERO && cashOperationContext.assetDisabled) {
            throw ValidationException (ValidationException.Validation.DISABLED_ASSET,
                    "Cash out operation (${cashOperationContext.uid}) for client ${cashOperationContext.clientId} asset ${walletOperation.assetId}, " +
                            "volume: ${NumberUtils.roundForPrint(walletOperation.amount)}: disabled asset")
        }
    }
}