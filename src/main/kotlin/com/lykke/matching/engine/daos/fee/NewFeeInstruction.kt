package com.lykke.matching.engine.daos.fee

import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.daos.FeeInstruction
import java.math.BigDecimal

open class NewFeeInstruction(type: FeeType,
                             takerSizeType: FeeSizeType?,
                             takerSize: BigDecimal?,
                             sourceClientId: String?,
                             targetClientId: String?,
                             val assetIds: List<String>) : FeeInstruction(type, takerSizeType, takerSize, sourceClientId, targetClientId) {

    companion object {
        fun create(fees: List<ProtocolMessages.Fee>): List<NewFeeInstruction> {
            return fees.map { create(it) }
        }

        fun create(fee: ProtocolMessages.Fee): NewFeeInstruction {
            val feeType = FeeType.getByExternalId(fee.type)
            var sizeType: FeeSizeType? = if (fee.hasSizeType()) FeeSizeType.getByExternalId(fee.sizeType) else null
            if (feeType != FeeType.NO_FEE && sizeType == null) {
                sizeType = FeeSizeType.PERCENTAGE
            }
            return NewFeeInstruction(
                    feeType,
                    sizeType,
                    if (fee.hasSize()) BigDecimal.valueOf(fee.size) else null,
                    if (fee.hasSourceClientId()) fee.sourceClientId else null,
                    if (fee.hasTargetClientId()) fee.targetClientId else null,
                    fee.assetIdList.toList()
            )
        }
    }

    override fun toString(): String {
        return "NewFeeInstruction(type=$type" +
                (if (sizeType != null) ", sizeType=$sizeType" else "") +
                (if (size != null) ", size=$size" else "") +
                (if (assetIds.isNotEmpty()) ", assetIds=$assetIds" else "") +
                (if (sourceClientId?.isNotEmpty() == true) ", sourceClientId=$sourceClientId" else "") +
                "${if (targetClientId?.isNotEmpty() == true) ", targetClientId=$targetClientId" else ""})"
    }

    override fun toNewFormat() = this
}