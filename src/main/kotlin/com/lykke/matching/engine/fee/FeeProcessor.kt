package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.FeeType
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

    fun processFee(feeInstruction: FeeInstruction?, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>): FeeTransfer? {
        if (feeInstruction == null || feeInstruction.type == FeeType.NO_FEE
                || feeInstruction.size == null || !(feeInstruction.size > 0 && feeInstruction.size < 1)
                || feeInstruction.targetClientId == null) {
            return null
        }
        val asset = assetsHolder.getAsset(receiptOperation.assetId)
        val feeSize = RoundingUtils.round(receiptOperation.amount * feeInstruction.size, asset.accuracy, true)
        when (feeInstruction.type) {
            FeeType.CLIENT_FEE -> {
                return processClientFee(feeInstruction, receiptOperation, operations, feeSize)
            }
            FeeType.EXTERNAL_FEE -> {
                if (feeInstruction.sourceClientId == null) {
                    return null
                }
                return if (balancesHolder.getAvailableBalance(feeInstruction.sourceClientId, receiptOperation.assetId) < feeSize) {
                    processClientFee(feeInstruction, receiptOperation, operations, feeSize)
                } else {
                    operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.sourceClientId, receiptOperation.assetId, receiptOperation.dateTime, -feeSize))
                    operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId, receiptOperation.assetId, receiptOperation.dateTime, feeSize))
                    FeeTransfer(receiptOperation.externalId, feeInstruction.sourceClientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeSize, receiptOperation.assetId)
                }
            }
            else -> {
                LOGGER.error("Unknown fee type: ${feeInstruction.type}")
                return null
            }
        }
    }

    private fun processClientFee(feeInstruction: FeeInstruction, receiptOperation: WalletOperation, operations: MutableList<WalletOperation>, feeSize: Double): FeeTransfer {
        operations.remove(receiptOperation)
        operations.add(WalletOperation(receiptOperation.id, receiptOperation.externalId, receiptOperation.clientId, receiptOperation.assetId, receiptOperation.dateTime, receiptOperation.amount - feeSize))
        operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, receiptOperation.assetId, receiptOperation.dateTime, feeSize))
        return FeeTransfer(receiptOperation.externalId, receiptOperation.clientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeSize, receiptOperation.assetId)
    }

}