package com.lykke.matching.engine.daos.v2

import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.messages.ProtocolMessages
import java.io.Serializable
import java.math.BigDecimal

open class FeeInstruction(
        val type: FeeType,
        val sizeType: FeeSizeType?,
        val size: BigDecimal?,
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
                    if (fee.hasSize()) BigDecimal.valueOf(fee.size)  else null,
                    if (fee.hasSourceClientId()) fee.sourceClientId else null,
                    if (fee.hasTargetClientId()) fee.targetClientId else null
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

    open fun toNewFormat(): NewFeeInstruction = NewFeeInstruction(type, sizeType, size, sourceClientId, targetClientId, emptyList())
}