package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.fee.Fee
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import java.util.LinkedList

fun listOfFee(fee: FeeInstruction?, fees: List<NewFeeInstruction>?): List<FeeInstruction> {
    val result = LinkedList<FeeInstruction>()
    fee?.let { result.add(it) }
    fees?.let { result.addAll(it) }
    return result
}

fun singleFeeTransfer(feeInstruction: FeeInstruction?, fees: List<Fee>) = if (feeInstruction != null && fees.isNotEmpty()) fees.first().transfer else null

fun checkFee(feeInstruction: FeeInstruction?, feeInstructions: List<NewFeeInstruction>?, assetPair: AssetPair? = null): Boolean {
    if (feeInstruction != null && feeInstructions?.isNotEmpty() == true) return false
    if (assetPair == null) return true
    listOfFee(feeInstruction, feeInstructions).forEach {
        if (!checkFee(it, assetPair)) {
            return false
        }
    }
    return true
}

private fun checkFee(feeInstruction: FeeInstruction, assetPair: AssetPair): Boolean {
    if (feeInstruction is NewFeeInstruction) {
        feeInstruction.assetIds.forEach { assetId ->
            if (assetPair.baseAssetId != assetId && assetPair.quotingAssetId != assetId) {
                return false
            }
        }
    }
    return true
}