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
            return FeeInstruction(
                    FeeType.getByExternalId(fee.type),
                    FeeSizeType.getByExternalId(fee.sizeType),
                    fee.size,
                    fee.sourceClientId,
                    fee.targetClientId
            )
        }
    }

    override fun toString(): String {
        return "FeeInstruction(type=$type " +
                "${if (sizeType != null) ", sizeType=$sizeType" else ""} " +
                "${if (size != null) ", size=$size" else ""} " +
                "${if (sourceClientId != null) ", sourceClientId=$sourceClientId" else ""} " +
                "${if (targetClientId != null) ", targetClientId=$targetClientId" else ""})"
    }
}