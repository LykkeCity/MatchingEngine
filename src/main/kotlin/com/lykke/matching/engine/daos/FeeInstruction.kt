package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.ProtocolMessages

open class FeeInstruction(
        val type: FeeType,
        val size: Double?,
        val sourceClientId: String?,
        val targetClientId: String?
) {

    companion object {
        fun create(fee: ProtocolMessages.Fee?): FeeInstruction? {
            if (fee == null) {
                return null
            }
            return FeeInstruction(FeeType.getByExternalId(fee.type), fee.size, fee.sourceClientId, fee.targetClientId)
        }
    }
}