package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.ProtocolMessages

class LimitOrderFeeInstruction(
        type: FeeType,
        takerSizeType: FeeSizeType?,
        takerSize: Double?,
        val makerSizeType: FeeSizeType?,
        val makerSize: Double?,
        sourceClientId: String?,
        targetClientId: String?
) : FeeInstruction(type, takerSizeType, takerSize, sourceClientId, targetClientId) {

    companion object {
        fun create(fee: ProtocolMessages.LimitOrderFee?): LimitOrderFeeInstruction? {
            if (fee == null) {
                return null
            }
            return LimitOrderFeeInstruction(
                    FeeType.getByExternalId(fee.type),
                    FeeSizeType.getByExternalId(fee.takerSizeType),
                    fee.takerSize,
                    FeeSizeType.getByExternalId(fee.makerSizeType),
                    fee.makerSize,
                    fee.sourceClientId,
                    fee.targetClientId)
        }
    }

    override fun toString(): String {
        return "LimitOrderFeeInstruction(type=$type" +
                (if (sizeType != null) ", takerSizeType=$sizeType" else "") +
                (if (size != null) ", takerSize=$size" else "") +
                (if (makerSizeType != null) ", makerSizeType=$makerSizeType" else "") +
                (if (makerSize != null) ", makerSize=$makerSize" else "") +
                (if (sourceClientId?.isNotEmpty() == true) ", sourceClientId=$sourceClientId" else "") +
                "${if (targetClientId?.isNotEmpty() == true) ", targetClientId=$targetClientId" else ""})"
    }

}