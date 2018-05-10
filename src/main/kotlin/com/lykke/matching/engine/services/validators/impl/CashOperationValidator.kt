package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CashOperationValidator @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                     private val assetsHolder: AssetsHolder,
                                                     private val applicationSettingsCache: ApplicationSettingsCache){

    fun performValidation(messageWrapper: MessageWrapper){
        isAssetEnabled(messageWrapper)
        isBalanceValid(messageWrapper)
        isAccuracyValid(messageWrapper)
    }

    private fun isBalanceValid(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        if (message.amount < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (balance - reservedBalance < Math.abs(message.amount)) {
                throw ValidationException("Cash out operation (${message.uid})" +
                        "for client ${message.clientId} asset ${message.assetId}, " +
                        "volume: ${NumberUtils.roundForPrint(message.amount)}: " +
                        "low balance $balance, reserved balance $reservedBalance", ValidationException.Validation.INVALID_BALANCE)
            }
        }
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper){
        val message = getMessage(messageWrapper)

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(message.amount, assetsHolder.getAsset(message.assetId).accuracy)

        if (!volumeValid) {
            throw ValidationException("Amount accuracy is invalid clientId: ${message.clientId}, amount  $message.amount",
                    ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun isAssetEnabled(messageWrapper: MessageWrapper){
        val message = getMessage(messageWrapper)
        if (message.amount < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            throw ValidationException ("Cash out operation (${message.uid}) for client ${message.clientId} asset ${message.assetId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.amount)}: disabled asset", ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashOperation {
        return messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
    }
}

