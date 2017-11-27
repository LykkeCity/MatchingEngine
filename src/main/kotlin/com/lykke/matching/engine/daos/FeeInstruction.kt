package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.ProtocolMessages
import java.io.Serializable

open class FeeInstruction(
        val type: FeeType,
        val sizeType: FeeSizeType?,
        val size: Double?,
        val sourceClientId: String?,
        val targetClientId: String?
) : Serializable {

    companion object {
        fun create(fee: ProtocolMessages.Fee?): FeeInstruction? {
            if (fee == null) {
                return null
            }
            val feeType = FeeType.getByExternalId(fee.type)
            var sizeType: FeeSizeType? = if (fee.hasSizeType()) FeeSizeType.getByExternalId(fee.sizeType) else null
            if (feeType != FeeType.NO_FEE && sizeType == null) {
                sizeType = FeeSizeType.PERCENTAGE
            }
            return FeeInstruction(
                    feeType,
                    sizeType,
                    if (fee.hasSize()) fee.size else null,
                    fee.sourceClientId,
                    fee.targetClientId
            )
        }
    }

    override fun toString(): String {
        return "FeeInstruction(type=$type" +
                (if (sizeType != null) ", sizeType=$sizeType" else "") +
                (if (size != null) ", size=$size" else "") +
                (if (sourceClientId?.isNotEmpty() == true) ", sourceClientId=$sourceClientId" else "") +
                "${if (targetClientId?.isNotEmpty() == true) ", targetClientId=$targetClientId" else ""})"
    }
}