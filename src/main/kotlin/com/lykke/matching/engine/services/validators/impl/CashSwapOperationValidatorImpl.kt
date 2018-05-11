package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashSwapOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class CashSwapOperationValidatorImpl @Autowired constructor(private val balancesHolder: BalancesHolder,
                                                            private val assetsHolder: AssetsHolder) : CashSwapOperationValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(message: ProtocolMessages.CashSwapOperation, operationId: String) {
        val operation = SwapOperation(operationId, message.id, Date(message.timestamp),
                message.clientId1, message.assetId1, message.volume1,
                message.clientId2, message.assetId2, message.volume2)

        isBalanceValid(message.clientId1, message.assetId1, message.volume1, operation)
        isBalanceValid(message.clientId2, message.assetId2, message.volume2, operation)

        isAccuracyValid(message.assetId1, message.volume1, operation)
        isAccuracyValid(message.assetId2, message.volume2, operation)
    }

    private fun isBalanceValid(client: String, assetId: String,
                               volume: Double, operation: SwapOperation){
        val balance = balancesHolder.getBalance(client, assetId)
        val reservedBalance = balancesHolder.getReservedBalance(client, assetId)
        if (balance - reservedBalance < operation.volume1) {
            LOGGER.info("Cash swap operation failed due to low balance: $client, $volume $assetId")
            throw ValidationException(ValidationException.Validation.LOW_BALANCE, "ClientId:$client,asset:$assetId, volume:$volume")
        }
    }

    private fun isAccuracyValid(assetId: String, volume: Double, operation: SwapOperation) {
        val volumeValid = NumberUtils.isScaleSmallerOrEqual(volume, assetsHolder.getAsset(assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("Volume accuracy invalid, assetId: $assetId, clientId1 ${operation.clientId1}, " +
                    "clientId2 ${operation.clientId2}, volume $volume")
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }
}