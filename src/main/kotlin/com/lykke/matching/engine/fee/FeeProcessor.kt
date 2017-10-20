package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.UUID

class FeeProcessor(private val balancesHolder: BalancesHolder,
                   private val assetsHolder: AssetsHolder) {

    companion object {
        private val LOGGER = Logger.getLogger(FeeProcessor::class.java.name)
    }

    fun processMakerFee(feeInstruction: FeeInstruction?, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>): FeeTransfer? {
        if (feeInstruction !is LimitOrderFeeInstruction?) {
            return null
        }
        return processFee(feeInstruction, receiptOperation, operations, feeInstruction?.makerSize)
    }

    fun processFee(feeInstruction: FeeInstruction?, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>): FeeTransfer? {
        return processFee(feeInstruction, receiptOperation, operations, feeInstruction?.size)
    }

    private fun processFee(feeInstruction: FeeInstruction?, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>, feeSize: Double?): FeeTransfer? {
        if (feeInstruction == null || feeInstruction.type == FeeType.NO_FEE
                || feeSize == null || !(feeSize > 0 && feeSize < 1)
                || feeInstruction.targetClientId == null) {
            return null
        }
        val asset = assetsHolder.getAsset(receiptOperation.assetId)
        val feeAmount = RoundingUtils.round(receiptOperation.amount * feeSize, asset.accuracy, true)
        when (feeInstruction.type) {
            FeeType.CLIENT_FEE -> {
                return processClientFee(feeInstruction, receiptOperation, operations, feeAmount, asset.accuracy)
            }
            FeeType.EXTERNAL_FEE -> {
                if (feeInstruction.sourceClientId == null) {
                    return null
                }
                return if (balancesHolder.getAvailableBalance(feeInstruction.sourceClientId, receiptOperation.assetId) < feeAmount) {
                    processClientFee(feeInstruction, receiptOperation, operations, feeAmount, asset.accuracy)
                } else {
                    operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.sourceClientId, receiptOperation.assetId, receiptOperation.dateTime, -feeAmount, isFee = true))
                    operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId, receiptOperation.assetId, receiptOperation.dateTime, feeAmount, isFee = true))
                    FeeTransfer(receiptOperation.externalId, feeInstruction.sourceClientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, receiptOperation.assetId)
                }
            }
            else -> {
                LOGGER.error("Unknown fee type: ${feeInstruction.type}")
                return null
            }
        }
    }

    private fun processClientFee(feeInstruction: FeeInstruction, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>, feeAmount: Double, accuracy: Int): FeeTransfer {
        operations.remove(receiptOperation)
        operations.add(WalletOperation(receiptOperation.id, receiptOperation.externalId, receiptOperation.clientId, receiptOperation.assetId, receiptOperation.dateTime, RoundingUtils.parseDouble(receiptOperation.amount - feeAmount, accuracy).toDouble()))
        operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, receiptOperation.assetId, receiptOperation.dateTime, feeAmount, isFee = true))
        return FeeTransfer(receiptOperation.externalId, receiptOperation.clientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, receiptOperation.assetId)
    }

}