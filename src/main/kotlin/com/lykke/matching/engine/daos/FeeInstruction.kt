package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.ProtocolMessages
import java.io.Serializable

open class FeeInstruction(
        val type: FeeType,
        val size: Double?,
        val sourceClientId: String?,
        val targetClientId: String?
): Serializable {

    companion object {
        fun create(fee: ProtocolMessages.Fee?): FeeInstruction? {
            if (fee == null) {
                return null
            }
            return FeeInstruction(FeeType.getByExternalId(fee.type), fee.size, fee.sourceClientId, fee.targetClientId)
        }
    }

    override fun toString(): String {
        return "FeeInstruction(type=$type " +
                "${if (size != null) ", size=$size" else ""} " +
                "${if (sourceClientId != null) ", sourceClientId=$sourceClientId" else ""} " +
                "${if (targetClientId != null) ", targetClientId=$targetClientId" else ""})"
    }
}