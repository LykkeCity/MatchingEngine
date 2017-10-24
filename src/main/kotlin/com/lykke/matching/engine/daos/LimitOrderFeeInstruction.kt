package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.ProtocolMessages

class LimitOrderFeeInstruction(
        type: FeeType,
        takerSize: Double?,
        val makerSize: Double?,
        sourceClientId: String?,
        targetClientId: String?
) : FeeInstruction(type, takerSize, sourceClientId, targetClientId) {

    companion object {
        fun create(fee: ProtocolMessages.LimitOrderFee?): LimitOrderFeeInstruction? {
            if (fee == null) {
                return null
            }
            return LimitOrderFeeInstruction(FeeType.getByExternalId(fee.feeCommon.type.number), fee.takerSize, fee.makerSize, fee.feeCommon.sourceClientId, fee.feeCommon.targetClientId)
        }
    }
}