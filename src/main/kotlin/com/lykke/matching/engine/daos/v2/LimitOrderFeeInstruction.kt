package com.lykke.matching.engine.daos.v2

import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.messages.ProtocolMessages
import java.math.BigDecimal

class LimitOrderFeeInstruction(
        type: FeeType,
        takerSizeType: FeeSizeType?,
        takerSize: BigDecimal?,
        val makerSizeType: FeeSizeType?,
        val makerSize: BigDecimal?,
        sourceClientId: String?,
        targetClientId: String?
) : FeeInstruction(type, takerSizeType, takerSize, sourceClientId, targetClientId) {

    companion object {
        fun create(fee: ProtocolMessages.LimitOrderFee?): LimitOrderFeeInstruction? {
            if (fee == null) {
                return null
            }
            val feeType = FeeType.getByExternalId(fee.type)
            var takerSizeType: FeeSizeType? = if (fee.hasTakerSize()) FeeSizeType.getByExternalId(fee.takerSizeType) else null
            var makerSizeType: FeeSizeType? = if (fee.hasMakerSizeType()) FeeSizeType.getByExternalId(fee.makerSizeType) else null
            if (feeType != FeeType.NO_FEE) {
                if (takerSizeType == null) {
                    takerSizeType = FeeSizeType.PERCENTAGE
                }
                if (makerSizeType == null) {
                    makerSizeType = FeeSizeType.PERCENTAGE
                }
            }
            return LimitOrderFeeInstruction(
                    feeType,
                    takerSizeType,
                    if (fee.hasTakerSize()) BigDecimal.valueOf(fee.takerSize) else null,
                    makerSizeType,
                    if (fee.hasMakerSize()) BigDecimal.valueOf(fee.makerSize) else null,
                    if (fee.hasSourceClientId()) fee.sourceClientId else null,
                    if (fee.hasTargetClientId()) fee.targetClientId else null)
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

    override fun toNewFormat() = NewLimitOrderFeeInstruction(type, sizeType, size, makerSizeType, makerSize, sourceClientId, targetClientId , emptyList(), null)

}