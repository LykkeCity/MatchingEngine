package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import java.util.LinkedList

fun listOfFee(fee: FeeInstruction?, fees: List<NewFeeInstruction>?): List<FeeInstruction> {
    val result = LinkedList<FeeInstruction>()
    fee?.let { result.add(it) }
    fees?.let { result.addAll(it) }
    return result
}

fun singleFeeTransfer(fee: FeeInstruction?, feeTransfers: List<FeeTransfer>) = if (fee != null && feeTransfers.isNotEmpty()) feeTransfers.first() else null

fun checkFee(fee: FeeInstruction?, fees: List<NewFeeInstruction>?, assetPair: AssetPair? = null): Boolean {
    if (fee != null && fees?.isNotEmpty() == true) return false
    if (assetPair == null) return true
    listOfFee(fee, fees).forEach {
        if (!checkFee(it, assetPair)) {
            return false
        }
    }
    return true
}

private fun checkFee(fee: FeeInstruction, assetPair: AssetPair): Boolean {
    if (fee is NewFeeInstruction) {
        fee.assetIds.forEach { assetId ->
            if (assetPair.baseAssetId != assetId && assetPair.quotingAssetId != assetId) {
                return false
            }
        }
    }
    return true
}